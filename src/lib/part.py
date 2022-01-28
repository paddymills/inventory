
from fractions import Fraction
from pyodbc import Row
from re import compile as regex

aliases = dict(
    mark    = ("Piecemark",),
    qty     = ("Qty",),
    desc    = ("Description",),
    type    = ("Commodity",),
    thk     = ("Thick",),
    wid     = ("Width",),
    len     = ("Length",),
    spec    = ("Specification",),
    grade   = ("Grade",),
    test    = ("ImpactTest",),
    remark  = ("Remark",),
    item    = ("Item",),
    dwg     = ("DwgNo",),
)

STOCK_GRADES = regex(r"^\w+-(50|345)W?(T|T2)?$")
STOCK_THK = [
    0.25,
    0.375,
    0.5,
    0.625,
    0.75,
    0.875,
    1.0,
]

def float_display(f, display_feet=False, force_zero=False):
    if display_feet and f >= 12.0:
        return "{:g}'-{}".format(f // 12, float_display(f % 12, force_zero=True))

    whole = int(f)
    frac = Fraction(f % 1)

    if whole == 0 and not force_zero:
        whole = ''

    if frac == 0:
        frac = ''

    return "{} {}".format(whole, frac).strip()


class Part:

    def __init__(self, init_data=None):
        if init_data is not None:
            self.parse_data(init_data)

    @property
    def matl_grade(self):
        if self.spec == 'A240 Type 304':
            return 'A240-304'

        if "HPS" in self.grade:
            zone = '3'
        elif not self.test:
            zone = ''
        else:
            zone = '2'

        if self.test == 'FCM':
            self.test = 'F'


        return "{}-{}{}{}".format(self.spec, self.grade, self.test, zone)

    @property
    def matl_grade_cvn(self):
        if self.test:
            return self.matl_grade

        return self.matl_grade + "T2"

    @property
    def for_prenest(self):
        if not STOCK_GRADES.match(self.matl_grade):
            return True
        if self.thk not in STOCK_THK:
            return True
        if self.wid > 95.0:
            return True
        if self.len > 240.0:
            return True

        return False

    def __repr__(self):
        _thk = float_display(self.thk)
        _wid = float_display(self.wid)
        _len = float_display(self.len, display_feet=True)

        return "Part<{}: {} x {} x {} [{}]>".format(self.mark, _thk, _wid, _len, self.matl_grade)

    def xml_format(self):
        return (self.mark, self.qty, self.thk, self.wid, self.len, self.matl_grade_cvn,
                self.item, self.dwg, None, None, None, None, self.remark, float_display(self.len * self.qty, display_feet=True))

    def parse_data(self, data):
        if type(data) is dict:
            self.__dict__.update(data)
        
        elif type(data) is Row:
            self._parse_row(data)

        else:
            raise NotImplementedError("unmatched data type")

    def _parse_row(self, row):
        header = [t[0] for t in row.cursor_description]

        for k, v in aliases.items():
            index = self.get_index_by_alias(header, v)
            setattr(self, k, row[index])

    def get_index_by_alias(self, row, aliases):
        for a in aliases:
            try:
                return row.index(a)
            except ValueError:
                pass

        raise IndexError("No index found for aliases: {}".format(aliases))
