
from collections import defaultdict
import sqlite3
from tabulate import tabulate
from tqdm import tqdm
import xlwings

wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\mb51.xlsx")
mb51 = wb.sheets.active.range("A2").expand().value
wb.close()

wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\cohv.xlsx")
cohv = wb.sheets.active.range("A2").expand().value
wb.close()

matl = mb51[2][2]
orders = [r[8] for r in mb51]
cnf_qty = defaultdict(int)
for row in cohv:
    order, part, qty = row[:3]
    if order in orders:
        cnf_qty[part] += qty

con = sqlite3.connect(r"C:\temp\sapprod.db")
cur = con.cursor()

output = list()
output.append("Part,Sap,Sn,Diff (Sn - Sap),Diff Area".split(","))
items = tqdm(sorted(cnf_qty.items()))
for part, qty in items:
    items.set_description("{:20}".format(part))
    cur.execute("""
        SELECT sum(qty)
        FROM sigmanest
        WHERE part=?
        AND matl=?
        AND isprod=1
    """, [part, matl])

    burned = cur.fetchone()[0] or 0
    diff = burned - qty

    if diff > 0:
        cur.execute("""
            SELECT part_qty, matl_qty
            FROM (
                SELECT part, part_qty, matl_qty FROM original
                UNION
                SELECT part, part_qty, matl_qty FROM sap_archive
            ) as x
            WHERE part=?
        """, [part])

        try:
            pq, mq = cur.fetchone()
            if pq:
                qty_ea = mq / pq
        except TypeError:
            qty_ea = 0
            
        output.append([part, qty, burned, diff, diff * qty_ea or None])

line = [None] * len(output[0])
line[0] = "total"
line[-1] = sum([r[-1] for r in output[1:] if r[-1]])

output.append(["\001"])
output.append(line)

print(tabulate(output, headers="firstrow", floatfmt=",.3f"))
