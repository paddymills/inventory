

from lib import db
from lib import parsers

from enum import Enum
from re import compile as regex
from tqdm import tqdm

PL_PREFIXES = ('PL', 'SHT', 'Sheet')
HD_WBS = regex(r"D-\d{7}-\d{5}")


class ChangeType(Enum):

    REMOVE  = 1
    UPDATE  = 2
    ADD     = 3
    PASS    = 4

class WbsMap:

    def __init__(self):
        self.items = list()
        self.jobs = set()

    def add(self, item, direction):
        self.items.append(MapPart(item, direction))
        self.jobs.add((item.part[:7], item.shipment))

    def get(self, part, wbs):
        for item in self.items:
            if item.part == part and item.wbs == wbs:
                return item

        raise KeyError

    def has_job(self, item):
        return (item.part[:7], item.shipment) in self.jobs

    def filter(self, filter_type):
        result = list()

        for item in self.items:
            if filter_type == item.group:
                result.append(item)

        return result


class MapPart:

    def __init__(self, data, direction=ChangeType.PASS):
        self.sql_id = data.id
        self.part = data.part
        self.shipment = data.shipment
        self.wbs = data.wbs
        self.qty = data.qty

        self.group = direction

        if self.qty == 0:
            self.group = ChangeType.PASS

    def __repr__(self):
        return "[{}] {} {} ({}) -> {}".format(self.sql_id, self.part, self.wbs, self.qty, self.group)

    @property
    def id(self):
        return "{}_{}".format(self.part, self.wbs)

def is_pl(desc):
    for prefix in PL_PREFIXES:
        if desc.startswith(prefix):
            return True

    return False


def main():
    with db.SndbConnection() as conn:      
        conn.cursor.execute(
            """
                SELECT
                    ID as id,
                    PartName AS part,
                    Shipment AS shipment,
                    WBS AS wbs,
                    QtyReq - QtyConf AS qty
                FROM
                    SAPPartWBS
            """)

        wbsmap = WbsMap()
        for row in conn.cursor.fetchall():
            wbsmap.add(row, ChangeType.REMOVE)

        cohv = parsers.SheetParser(sheet='active')
        with tqdm(desc="Parsing data", total=len(cohv.data_rng.rows)) as progress:
            for row in cohv.parse_sheet():
                progress.update()

                try:
                    row.shipment = int(row.shipment)
                except ValueError:
                    continue

                # skip row if not:
                #   - plate
                #   - HD WBS
                #   - a job/shipment in the current WBS map
                if is_pl(row.desc) and HD_WBS.match(row.wbs) and wbsmap.has_job(row):

                    try:
                        map_data = wbsmap.get(row.part, row.wbs)
                        if map_data.qty == row.qty:
                            map_data.group = ChangeType.PASS

                        else:
                            map_data.qty = row.qty
                            map_data.group = ChangeType.UPDATE

                    except KeyError:
                        wbsmap.add(row, ChangeType.ADD)

        # ~~~~~~~~~~~~~~~~~ update database ~~~~~~~~~~~~~~~~~

        # clear demand
        for row in tqdm(wbsmap.filter(ChangeType.REMOVE), desc="Removing old"):
            conn.cursor.execute(
                """
                    UPDATE  SAPPartWBS
                    SET     QtyConf=QtyReq
                    WHERE   ID=?
                """,
                row.sql_id
            )

        # update existing
        for row in tqdm(wbsmap.filter(ChangeType.UPDATE), desc="Updating Existing"):
            conn.cursor.execute(
                """
                    UPDATE  SAPPartWBS
                    SET     QtyConf=QtyReq-?
                    WHERE   ID=?
                """,
                row.qty,
                row.sql_id
            )

        # add new
        for row in tqdm(wbsmap.filter(ChangeType.ADD), desc="Adding new"):
            conn.cursor.execute(
                """
                    INSERT INTO SAPPartWBS (PartName, WBS, QtyReq, Shipment)
                    VALUES (?, ?, ?, ?)
                """,
                row.part, row.wbs, row.qty, row.shipment
            )

        # conn.cursor.commit()

if __name__ == "__main__":
    main()
