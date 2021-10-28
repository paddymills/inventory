
from lib import sndb
import xlwings

from re import compile as regex

LOCATION_RE = regex(r'([A-Za-z])(\d+)')


def main():
    dump_to_xl([
        dict(name="Plant 2 Yard",       data=main_yard()),
        dict(name="Detail Bay Yard",    data=detail_bay()),
        # dict(name="Staged or Burned",   data=staged_or_burned()),
    ])


def main_yard():
    locs = ["{}%".format(x) for x in "ABCDEFGS"]

    return pull_locs(locs)


def detail_bay():
    locs = ["{}%".format(x) for x in "HIJKLMNT"]

    return pull_locs(locs)


def staged_or_burned():
    data = dict()
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT
                Location AS loc,
                PrimeCode AS mm,
                Mill AS wbs,
                Qty * Area AS area
            FROM
                Stock
            WHERE
                Location='XPARTS'
        """)

        for row in cursor.fetchall():
            key = "{}_{}_{}".format(row.loc, row.mm, row.wbs)
            if key not in data:
                data[key] = [row.loc, row.mm, row.wbs, 0]

            data[key][-1] += row.area

    return [x for x in data.values()]


def zfill_loc(loc):
    try:
        row, num = LOCATION_RE.match(loc).group(1, 2)
    except TypeError:
        return loc

    return "{}{:02}".format(row, int(num))


def pull_locs(locs):
    data = dict()

    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()

        for loc in locs:
            cursor.execute("""
                SELECT
                    Location AS loc,
                    PrimeCode AS mm,
                    Mill AS wbs,
                    Qty * Area AS area,
                    Remark as uom
                FROM
                    Stock
                WHERE
                    SheetData4='10.18.21'
                AND
                    Location LIKE ?
            """, loc)

            for row in cursor.fetchall():
                key = "{}_{}_{}".format(row.loc, row.mm, row.wbs)
                if key not in data:
                    data[key] = [key, row.loc, row.mm, row.wbs, 0, row.uom]

                data[key][4] += row.area

    return sorted(data.values(), key=lambda x: (zfill_loc(x[0]), x[1], x[2]))


def dump_to_xl(datasets):
    wb = xlwings.Book()

    for i, data in enumerate(datasets):
        try:
            sheet = wb.sheets[i]
        except IndexError:
            sheet = wb.sheets.add(after=sheet)

        sheet.name = data["name"]
        sheet.range("A1").value = ["UID", "Location", "SAP MM", "WBS Element", "Qty", "UoM"]
        sheet.range("A2").value = data["data"]
        sheet.range("D:D").number_format = '0.000'
        sheet.autofit()


if __name__ == "__main__":
    main()
