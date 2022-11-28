
from lib.db import SndbConnection

from collections import defaultdict
from tqdm import tqdm
import tabulate
from re import compile as regex

INBOX_TEXT_1 = regex(r"Planned order not found for (\d{7}[a-zA-Z])-([\w-]+), [SD]-\d{7}-(\d{5}), ([\d,]+).000, Sigmanest Program:([\d-]+)")

class Line:

    def __init__(self, x):
        match = INBOX_TEXT_1.match(x)

        self.job  = match.group(1)
        self.mark = match.group(2)
        self.wbs  = match.group(3)
        self.qty  = int(match.group(4))
        self.prog = match.group(5)

    @property
    def part(self):
        return "{}-{}".format(self.job, self.mark)

    @property
    def wbs_elem(self):
        return "D-{}-{}".format(self.job, self.wbs)

    def prod_line(self, matl, matl_wbs, matl_qty_ea, loc):
        vals = [
            self.part,
            "S-{}".format(self.job),
            self.wbs_elem,
            "PROD",
            str(self.qty),
            "EA",
            matl,
            matl_wbs,
            str(round(self.qty * matl_qty_ea, 3)),
            "IN2",
            loc,
            "HS01",
            self.prog,
            "\n"
        ]

        return "\t".join(vals)

lines = []
with open("tmp/input.txt") as i:
    for l in i.readlines():
        lines.append(Line(l))

with open('tmp/parts.txt', 'w') as p:
    p.writelines(sorted(set([x.part + "\n" for x in lines])))

programs = list(set(sorted([(x, "%{}%{}".format(x.job[-4:], x.mark), x.prog) for x in lines], key=lambda x: (x[1], x[2]))))
counts = defaultdict(float)
output = list()
with SndbConnection() as db:
    for line, part, prog in tqdm(programs):
        db.cursor.execute("""
            SELECT
                StockHistory.PrimeCode,
                StockHistory.Mill,
                PIPArchive.RectArea,
                StockHistory.Location
            FROM PIPArchive
                INNER JOIN StockHistory
                    ON PIPArchive.ProgramName=StockHistory.ProgramName
                    AND PIPArchive.SheetName=StockHistory.SheetName
            WHERE
                PIPArchive.ProgramName=?
            AND
                PIPArchive.PartName LIKE ?
            AND
                PIPArchive.TransType = 'SN102'
        """, prog, part)

        res = db.cursor.fetchone()
        if not res:
            tqdm.write("Not found: {}".format(part))
            continue

        mm, wbs, qty, loc = res
        counts["{}:{}".format(mm, loc)] += qty * line.qty

        output.append(line.prod_line(*res))

temp = [(*k.split(":"), v) for k, v in counts.items()]
print(tabulate.tabulate(sorted(temp)))

with open("Production_now.ready", 'w') as pf:
    pf.writelines(output)
