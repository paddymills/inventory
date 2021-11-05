
from lib import sndb
import xlwings

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
                    data[key] = [None, row.loc, row.mm, row.wbs, 0, row.uom]

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
        for r in range(2, len(data["data"]) + 2):
            sheet.range(r, 1).formula = '=CONCATENATE(B{0},"_",C{0},"_",D{0})'.format(r)
        sheet.range("D:D").number_format = '0.000'
        sheet.autofit()


def do_compare():
    wb = xlwings.books.active
    xlwings.books["export.XLSX"].sheets[0].copy(after=wb.sheets[-1], name="SAP")
    
    sheet = wb.sheets["SAP"]
    sheet.range("A:A").insert(shift='right')
    sheet.range("A1").value = "UID"

    quantifiers = wb.sheets["SigmaNest"].range("B2:D2").expand('down').value
    for r in range(2, sheet.range("B2").expand('down').end('down').row + 1):
        if not sheet.range(r, 2).value:
            break

        sheet.range(r, 1).formula = '=CONCATENATE(E{0},"_",B{0},"_",I{0})'.format(r)
        quantifiers.append([sheet.range(r, x).value for x in (5, 2, 9)])

    sheet = wb.sheets.add("Compare")
    sheet.range("A1").value = ["UID", "SLoc Sort", "Location", "SAP MM", "WBS Element", "SigmaNest", "SAP", "SAP UoM", "Adj SAP Qty", "SAP/Sigmanest Δ"]
    for r, row in enumerate(sorted(set([tuple(q) for q in quantifiers]), key = lambda x: x[1]), start=2):
        sheet.range(r, 3).value = row
        sheet.range(r, 1).formula = '=CONCATENATE(C{0},"_",D{0},"_",E{0})'.format(r)
        sheet.range(r, 2).formula = '=CONCATENATE(LEFT(C{0},1),TEXT(MID(C{0},2,LEN(C{0})-1),"00"))'.format(r)
        sheet.range(r, 6).formula = '=VLOOKUP(A{0},SigmaNest!A:E,5,FALSE)'.format(r)
        sheet.range(r, 7).formula = '=VLOOKUP(A{0},SAP!A:F,6,FALSE)'.format(r)
        sheet.range(r, 8).formula = '=VLOOKUP(A{0},SAP!A:G,7,FALSE)'.format(r)
        sheet.range(r, 9).formula = '=IFERROR(IF(H{0}="IN2",G{0},G{0}*144),0)'.format(r)
        sheet.range(r, 10).formula = '=I{0}-F{0}'.format(r)

    sheet.autofit()
    sheet.range("J:J").number_format = "0,000.00"


if __name__ == "__main__":
    main()
