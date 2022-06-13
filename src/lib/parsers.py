
import xlwings

from multiprocessing import Pool
from os import path, listdir
from re import compile as regex
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
aliases.desc = ("Material description")

BASE_SAP_DATA_FILES = r"\\hssieng\SNData\SimTrans\SAP Data Files"

SCAN_PART = regex(r"(\d{3})([a-zA-Z])-\w+-\w+-(\w+)")
DATA_FILE_JOB = regex(r"S-(\d{7})")


def data_file_folder(folder_name):
    return path.join(BASE_SAP_DATA_FILES, folder_name)


def part_name(_part, _job):
    scan_match = SCAN_PART.match(_part)
    job_match = DATA_FILE_JOB.match(_job)
    if not (scan_match and job_match):
        return _part

    job_end, structure, part = scan_match.groups()
    job_without_structure = job_match.group(1)

    if not job_without_structure.endswith(job_end):
        return _part

    return "{}{}-{}".format(job_without_structure, structure, part)


class SheetParser:

    def __init__(self, sheet=None):

        self._header = None
        self._sheet = None

        if sheet:
            self.set_sheet(sheet)

    def set_sheet(self, sheet):

        if sheet is None:
            return

        elif sheet == 'active':
            self._sheet = xlwings.books.active.sheets.active
        
        elif type(sheet) is xlwings.Sheet:
            self._sheet = sheet

        elif type(sheet) in (str, int):
            self._sheet = xlwings.books.active.sheets[sheet]

        else:
            raise TypeError("sheet must be an xlwings sheet, string or integer")

    @property
    def header(self):

        if self._header is None:
            self.parse_header()

        return self._header

    @property
    def sheet(self):
        """
        returns sheet if set, otherwise active sheet

        this allows operation on active sheet without setting the stored sheet
        """
        return self._sheet or xlwings.books.active.sheets.active

    @property
    def max_col(self):

        return max(self.header.__dict__.values())

    @property
    def last_row(self):

        return self.sheet.range((2, self.header.matl + 1)).end('down').row

    @property
    def data_rng(self, start_row=2):
        return self.sheet.range((start_row, 1), (self.last_row, self.max_col + 1))

    def parse_header(self, row=None):

        self._header = SimpleNamespace()

        if row is None:
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

        self._header_parsed = True

    def parse_row(self, row):
        res = SimpleNamespace()

        for k, v in self.header.__dict__.items():
            # same as res.matl = row[header.matl]
            setattr(res, k, row[v])

        return res

    def parse_sheet(self, sheet=None, with_progress=False):
        """
            convenience method to parse entire sheet
        """

        if sheet:
            self.set_sheet(sheet)

        if self.header is None:
            self.parse_header()

        rng = self.data_rng.value

        if with_progress:
            for row in tqdm(rng, desc='Parsing sheet', total=len(rng)):
                yield self.parse_row(row)

        else:
            for row in rng:
                yield self.parse_row(row)


class CnfFileParser:

    def __init__(self, processed_only=False):
        self.ipart = SimpleNamespace(matl=0, qty=4, wbs=2, plant=11, job=1, program=12)
        self.imatl = SimpleNamespace(matl=6, qty=8, loc=10, wbs=7, plant=11)

        self.dirs = [
            data_file_folder("Processed"),
        ]

        if not processed_only:
            for d in ('deleted files', '_temp'):
                _dir = data_file_folder(d)
                if path.exists(_dir):
                    self.dirs.append(_dir)

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
                    part = part_name(*line[self.ipart.matl:self.ipart.job+1])
                    if part in parts:
                        prod_data.append(line)

        return prod_data

    def file_worker(self, f):
        result = list()
        with open(f, "r") as prod_file:
            for line in prod_file.readlines():
                result.append(line.upper().split("\t"))

        return result

    def parse_row(self, row):
        if type(row) is str:
            row = row.split("\t")

        return ParsedCnfRow(row, self.ipart, self.imatl)

class ParsedCnfRow:

    def __init__(self, row, part_indices, matl_indices):
        # part
        self.part_name = row[part_indices.matl]
        self.part_job = row[part_indices.job]
        self.part_wbs = row[part_indices.wbs]
        self.part_qty = int(row[part_indices.qty])

        # material
        self.matl_master = row[matl_indices.matl]
        self.matl_wbs = row[matl_indices.wbs]
        self.matl_qty = float(row[matl_indices.qty])
        self.matl_loc = row[matl_indices.loc]
        self.matl_plant = row[matl_indices.plant]

        self.program = row[part_indices.program]

        self.matl_qty_per_ea = self.matl_qty / self.part_qty

    def __repr__(self):
        return "<ParsedCnfRow> {}".format(self.ls())

    def ls(self):
        return [
            #part
            self.part_name,
            self.part_job,
            self.part_wbs,
            "PROD",
            str(self.part_qty),
            "EA",
            
            # material
            self.matl_master,
            self.matl_wbs,
            "{:.3f}".format( self.matl_qty ),
            "IN2",
            self.matl_loc,
            self.matl_plant,
            
            self.program, "\n"
        ]

    def output(self):
        return "\t".join(self.ls())

    def __add__(self, other):
        assert self.part_name == other.part_name, "Cannot add parts with different Marks"
        assert self.matl_master == other.matl_master, "Cannot add parts with different Material Masters"
        assert self.matl_wbs == other.matl_wbs, "Cannot add parts with different WBS Elements"

        self.part_qty += other.part_qty
        self.matl_qty += other.matl_qty

        self.matl_qty_per_ea = self.matl_qty / self.part_qty
