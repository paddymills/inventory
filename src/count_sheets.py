
import xlwings
import os
import sys
import re

from lib import sndb

XL_TEMPLATE_FILE = os.path.join(os.path.dirname(__file__), "template.xlsx")

ROUND_DIGITS = 3
IRREGULAR_SHAPE_THRESHOLD = 100.0


class Sheet:

    def __init__(self, sql_row):
        self.name = sql_row.SheetName
        self.mm = sql_row.PrimeCode
        self.thk = sql_row.Thickness
        self._wid = sql_row.Width
        self._len = sql_row.Length
        self.area = sql_row.Area
        # self.qty = sql_row.Qty
        self.type = sql_row.SheetType

    @property
    def len(self):
        return round(self._len, ROUND_DIGITS)

    @property
    def wid(self):
        return round(self._wid, ROUND_DIGITS)

    @property
    def is_new(self):
        return self.type == 0

    @property
    def is_remnant(self):
        return not self.is_new

    @property
    def is_non_rectangular(self):
        if abs(self.area - self._len * self._wid) > IRREGULAR_SHAPE_THRESHOLD:
            return True

        return False

    def __repr__(self):
        return "{} ({} x {} x {}) [{}]".format(self.mm, self.thk, self.wid, self.len, self.qty)

    def xl_format(self):
        row = [None, None, self.mm, self.thk, self.wid, self.len, None]

        if self.is_remnant:
            row[0] = "□"
            row[1] = self.name
        if self.is_non_rectangular:
            row[-1] = "✔"

        return row


def main():
    while True:
        loc = input("Location: ")
        if not loc:
            break

        validate_loc(loc.upper())


def validate_loc(loc):
    rng = r"([A-Z]+)(\d+)-(\d+)"
    single = r"^[A-Z]+\d+$"
    row = r"^[A-Z]+$"

    if re.match(single, loc):
        create_count_sheet(loc)
        return

    if match := re.match(rng, loc):
        r, start, end = match.groups()
        for i in range(int(start), int(end) + 1):
            create_count_sheet("{}{}".format(r, i))

    elif re.match(row, loc):
        for i in range(1, 20 + 1):
            create_count_sheet("{}{}".format(loc, i), fail_silent=True)

    else:
        print("Failed to match location:", loc)


def create_count_sheet(location, fail_silent=False):
    sheets = {}
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT * FROM Stock
            WHERE Location=?
        """, location)

        for row in cursor.fetchall():
            sheet = Sheet(row)

            if sheet.is_remnant:
                sheets[sheet.name] = sheet

            else:
                sheets[sheet.mm] = sheet

    if sheets:
        dump_to_xl([x.xl_format() for x in sheets.values()], location)
    elif not fail_silent:
        print("No data returned for location", location)


def dump_to_xl(data, location):
    wb = xlwings.Book(XL_TEMPLATE_FILE)
    sheet = wb.sheets["TEMPLATE"].copy(name=location)
    sheet.range("A2").value = data
    sheet.range("K1").value = location


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for loc in sys.argv[1:]:
            validate_loc(loc.upper())

    else:
        main()
