
import xlwings

from lib import sndb
from lib import printer

from argparse import ArgumentParser
from re import compile as regex

LOCATION_RE = regex(r'([A-Za-z])(\d+)')


def main():
    dmain = main_yard()
    dsecondary = detail_bay()

    dump_to_xl([
        # dict(name="Plant 2 Yard",       data=dmain),
        # dict(name="Detail Bay Yard",    data=dsecondary),
        dict(name="SigmaNest",          data=[*dmain, *dsecondary])
    ])

    if "export.XLSX" in [b.name for b in xlwings.books]:
        do_compare()


def main_yard():
    locs = ["{}%".format(x) for x in "ABCDEFGS"]

    return pull_locs(locs)


def detail_bay():
    locs = ["{}%".format(x) for x in "HIJKLMNT"]

    return pull_locs(locs)


def staged_or_burned():
    data = dict()
    with sndb.SndbConnection() as db:
        db.cursor.execute("""
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

        for row in db.cursor.fetchall():
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

    with sndb.SndbConnection() as db:
        for loc in locs:
            db.cursor.execute("""
                SELECT
                    Location AS loc,
                    PrimeCode AS mm,
                    Mill AS wbs,
                    Qty * Area AS area,
                    Remark as uom
                FROM
                    Stock
                WHERE
                    Location LIKE ?
                AND
                    SheetName NOT LIKE 'W%'
            """, loc)

            for row in db.cursor.fetchall():
                key = "{}_{}_{}".format(row.loc, row.mm, row.wbs)
                if key not in data:
                    data[key] = [row.loc, row.mm, row.wbs, 0, row.uom]

                data[key][3] += row.area

    return sorted(data.values(), key=lambda x: (zfill_loc(x[0]), x[1], x[2]))


def dump_to_xl(datasets):
    wb = xlwings.Book()

    for i, data in enumerate(datasets):
        try:
            sheet = wb.sheets[i]
        except IndexError:
            sheet = wb.sheets.add(after=sheet)

        sheet.name = data["name"]
        sheet.range("A1").value = ["Location", "SAP MM", "WBS Element", "Qty", "UoM"]
        sheet.range("A2").value = data["data"]
        sheet.range("D:D").number_format = '0.000'
        sheet.autofit()


def do_compare():
    wb = xlwings.books.active
    xlwings.books["export.XLSX"].sheets[0].copy(after=wb.sheets[-1], name="SAP")

    sheet = wb.sheets["SAP"]

    quantifiers = wb.sheets["SigmaNest"].range("A2:C2").expand('down').value
    for r in range(2, sheet.range("A2").expand('down').end('down').row + 1):
        if not sheet.range(r, 2).value:
            break

        quantifiers.append([sheet.range(r, x).value for x in (4, 1, 8)])

    sheet = wb.sheets.add("Compare", after=wb.sheets[-1])
    sheet.range("A1").value = ["Location", "SAP MM", "WBS Element"]
    sheet.range("A1").value = quantifiers

    sheet.autofit()


def show_part(part, as_csv=False):
    with sndb.SndbConnection(func="wbsmap") as db:
        db.cursor.execute("""
            SELECT * FROM SAPPartWBS
            WHERE PartName=?
        """, part)

        data = db.collect_table_data()

    printer.print_to_source(data)


def get_last_10_burned(as_csv=False):
    with sndb.SndbConnection() as db:
        db.cursor.execute("""
            SELECT TOP 10
                CompletedDateTime AS Timestamp,
                ProgramName AS Program,
                SheetName AS Sheet,
                MachineName AS Machine
            FROM CompletedProgram
            WHERE MachineName NOT LIKE 'Plant_3_%'
            ORDER BY CompletedDateTime DESC
        """)

        res = db.collect_table_data()
        printer.print_to_source(res, as_csv)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Return as csv output")
    parser.add_argument("--part", help="part to find wbs map for")
    parser.add_argument("--last", action='store_true', help="part to find wbs map for")
    args = parser.parse_args()

    if args.part:
        show_part(args.part, as_csv=args.csv)
    elif args.last:
        get_last_10_burned(as_csv=args.csv)

    else:
        main()
