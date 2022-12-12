
import os
import xlwings

from lib import parsers
from collections import defaultdict
from tabulate import tabulate
from tqdm import tqdm
from types import SimpleNamespace

ipart = SimpleNamespace(matl=0, qty=4, wbs=2, plant=11, job=1, program=12)
imatl = SimpleNamespace(matl=6, qty=8, loc=10, wbs=7, plant=11)
parsed = dict()

def parse_file(f, parse_func):
    with open(f, "r") as prod_file:
        for line in prod_file.readlines():
            row = line.strip().upper().split("\t")
            yield parse_func(row)


for f in os.scandir(os.path.expanduser("~\\Downloads\\Sent")):
    if f.name.startswith("Production_"):
        pf = lambda x: parsers.ParsedCnfRow(x, ipart, imatl)
    elif f.name.startswith("Issue_"):
        pf = lambda x: parsers.ParsedIssueRow(x)
    else:
        continue

    parsed[f.name] = parse_file(f, pf)

s = xlwings.books.active.sheets['data']
end = 266
s.range((2, 8), (end, 8)).clear()

data = dict()
for i, row in enumerate(s.range((2, 1), (end, 7)).value, start=2):
    data[i] = row

issued = defaultdict(float)
for f, p in parsed.items():
    progress = tqdm(p, desc=f)
    for row in progress:
        if type(row) is parsers.ParsedCnfRow:
            for i, line in data.items():
                if line[0] == row.matl_master:
                    if line[3] == row.part_name:
                        s.range((i, 8)).value = "MATCHED"
                        issued[row.matl_master] += line[1]
                        del data[i]
                        break
            else:
                progress.write("Cnf not found: {}\t({})".format(row.part_name, row.matl_master))
        elif type(row) is parsers.ParsedIssueRow:
            for i, line in data.items():
                if line[0] == row.matl:
                    if line[5] == row.program:
                        if line[1] == row.qty:
                            s.range((i, 8)).value = "MATCHED"
                            issued[row.matl] += line[1]
                            del data[i]
                            break
            else:
                progress.write("Issue not found: {}\t({})".format(row.matl, row.program))

print(tabulate(sorted(issued.items()), headers=["Material", "Qty Issued"]))
