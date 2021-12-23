
import re
import xlwings

from multiprocessing import Pool
from os import path, listdir
from types import SimpleNamespace
from tqdm import tqdm

aliases = SimpleNamespace()
aliases.matl = ("Material", "Material Number")
aliases.part = aliases.matl
aliases.mark = aliases.matl
aliases.qty = ("Qty in unit of entry", "Unrestricted", "Order quantity (GMEIN)")
aliases.shipment = ("Occurrence")
aliases.type = ("Order Type")
aliases.loc = ("Storage Location")
aliases.wbs = ("WBS Element", "Special stock number")
aliases.plant = ("Plant")
aliases.order = ("Order")

BASE_SAP_DATA_FILES = r"\\hssieng\SNData\SimTrans\SAP Data Files"

SCAN_PATTERN = re.compile(r"\d{3}[A-Z]-\w+-\w+-(\w+)", re.IGNORECASE)


class SheetParser:

    def __init__(self, header_data=None, sheet=None):

        self.header = None
        self.sheet = sheet or xlwings.books.active.sheets.active

        if header_data:
            self.parse_header(header_data)
        else:
            self.parse_header()

    @property
    def max_col(self):
        return max(self.header.__dict__.values())

    def parse_header(self, row=None, range=None):

        self.header = SimpleNamespace()

        if row is None:
            if range:
                if type(range) is str:
                    range = self.sheet.range(range)
                row = range.expand('right').value

            else:
                row = self.sheet.range("A1").expand('right').value

        for i, item in enumerate(row):
            # same as
            # `
            #   if item in aliases.matl:
            #       header.matl = i
            # `
            # just less code to maintain
            for k, v in aliases.__dict__.items():
                if item in v:
                    setattr(self.header, k, i)

    def parse_row(self, row):
        res = SimpleNamespace()

        for k, v in self.header.__dict__.items():
            # same as res.matl = row[header.matl]
            setattr(res, k, row[v])

        return res

    def parse_sheet(self, sheet=None):
        """
            convenience method to parse entire sheet
        """

        if sheet is None:
            sheet = self.sheet or xlwings.books.active.sheets.active

        last_row = sheet.range((2, self.header.matl + 1)).end('down').row
        rng = sheet.range((2, 1), (last_row, self.max_col + 1)).value

        for row in rng:
            yield self.parse_row(row)


class CnfFileParser:

    def __init__(self, processed_only=False):
        self.ipart = SimpleNamespace(matl=0, qty=4, wbs=2, plant=11, job=1)
        self.imatl = SimpleNamespace(matl=6, qty=8, loc=10, wbs=7, plant=11)

        self.dirs = [
            path.join(BASE_SAP_DATA_FILES, "Processed"),
        ]

        if not processed_only:
            self.dirs += [
                path.join(BASE_SAP_DATA_FILES, "deleted files"),
                path.join(BASE_SAP_DATA_FILES, "_temp"),
            ]

    @property
    def files(self):
        for d in self.dirs:
            for f in listdir(d):
                yield path.join(d, f)

    def get_cnf_file_rows(self, parts):
        prod_data = list()
        num_files = sum([len(listdir(x)) for x in self.dirs])
        with tqdm(desc="Fetching Data", total=num_files) as pbar:
            for processed_file in Pool().imap(self.file_worker, self.files):
                pbar.update()
                for line in processed_file:
                    # match scan parts and change
                    match = SCAN_PATTERN.match(line[self.ipart.matl])
                    if match:
                        line[self.ipart.matl] = "{}-{}".format(line[self.ipart.job][2:], match.group(1))

                    if line[self.ipart.matl] in parts:
                        prod_data.append(line)

        return prod_data

    def file_worker(self, f):
        result = list()
        with open(f, "r") as prod_file:
            for line in prod_file.readlines():
                result.append(line.upper().split("\t"))

        return result
