
import csv

from collections import defaultdict
from tabulate import tabulate

from lib.db import SndbConnection
from lib.parsers import CnfFileParser


class Part:

    def __init__(self, name):
        self.name = name
        self.inbox = 0
        self.burned = 0
        self.cnf = 0

        self._inbox = list()
        self.orders = defaultdict(int)

    def tabular(self):
        return [self.name, self. burned, self.cnf, self.inbox, self.burned - self.cnf - self.inbox]

    def add_entry(self, entry):
        # print("Adding {}({}) to...".format(entry.program, entry.part_qty))
        # for row in self._inbox:
        #     print("\t", row)

        for n in self._inbox:
            if entry.program == n.program:
                try:
                    n += entry
                    return
                except AssertionError:  # mismatch in material master and/or wbs
                    pass

        self._inbox.append(entry)

    def fix(self):
        fixed = list()
        not_fixed = list()
        for p in sorted(self._inbox, key=lambda x: x.part_qty):
            for wbs, qty in sorted(self.orders.items(), key=lambda x: x[1]):
                if p.part_qty <= qty:
                    p.part_wbs = wbs
                    fixed.append(p.output())

                    self.orders[wbs] -= p.part_qty
                    break

                elif qty > 0:
                    before_qty = p.part_qty

                    p.part_qty = qty
                    p.matl_qty = p.matl_qty_per_ea * qty
                    fixed.append(p.output())

                    self.orders[wbs] = 0
                    p.part_qty = before_qty - qty
                    p.matl_qty = p.matl_qty_per_ea * p.part_qty
                    if p.part_qty == 0:
                        break
            else:
                not_fixed.append(p.output())
        
        return fixed, not_fixed


def main():
    parts = get_data(from_sndb=False)

    # show data
    # print(tabulate(
    #     [v.tabular() for v in parts.values()],
    #     headers=["Part", "Burned", "Cnf", "Inbox", "Delta"]
    # ))

    # try to fix things
    fixed = open("tmp/fixed.ready", "w")
    not_fixed = open("tmp/not_fixed.ready", "w")

    for part in parts.values():
        f, n = part.fix()
        fixed.writelines(f)
        not_fixed.writelines(n)

    fixed.close()
    not_fixed.close()


def get_data(from_sndb=True):
    parts = dict()

    # get inbox errors
    with open("tmp/inbox.ready", 'r') as f:
        parser = CnfFileParser()
        for line in f.readlines():
            parsed = parser.parse_row(line.upper())

            if parsed.part_name not in parts:
                parts[parsed.part_name] = Part(parsed.part_name)

            part = parts[parsed.part_name]

            part.inbox += parsed.part_qty
            part.add_entry(parsed)

    if from_sndb:
        # get number of each part burned from database
        with SndbConnection() as db:
            for part in parts:
                sn_part = part.replace("-", "_", 1)
                db.cursor.execute("""
                    SELECT SUM(QtyInProcess)
                    FROM PIPArchive
                    WHERE TransType='SN102'
                    GROUP BY PartName
                    HAVING PartName = ?
                """, sn_part)

                result = db.cursor.fetchone()
                if result:
                    parts[part].burned += result[0]

    # get number of each part confirmed from export.csv
    with open("tmp/export.csv", "r") as export:
        for row in csv.DictReader(export):
            part = row["Material Number"]
            wbs = row["WBS Element"]
            qty = int(row["Order quantity (GMEIN)"])

            order_type = row["Order Type"]

            if order_type == "PP01":
                parts[part].cnf += qty
            elif order_type == "PR":
                parts[part].orders[wbs] += qty
            else:
                print("unmatched order type:", order_type)

    return parts

if __name__ == "__main__":
    main()
