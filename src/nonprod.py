
from collections import defaultdict
from datetime import datetime
import sqlite3
from tabulate import tabulate
from tqdm import tqdm
import xlwings

start = datetime(2022, 1, 1)


def main():
    materials = [
        "50/50W-0004",
        "50/50W-0006",
        "50/50W-0008",
        "50/50W-0010",
        "50/50W-0012",
        "50/50W-0014",
        "50/50W-0100",
        "50/50W-0104",
        "50/50W-0106",
        "50/50W-0108",
    ]
    original_mb51, cohv = get_data()

    for matl in materials:

        mb51, sn = get_matl_data(original_mb51, cohv, matl)

        mb51 = reversals(mb51)
        # issued = gi_by_program(mb51)
        # not_cnf = successes(mb51, sn, issued)

        try_both(mb51, sn, matl)



def get_data():
    cohv = dict()
    for f in ('cohv', 'cohvsn'):
        wb = xlwings.Book("C:/Users/PMiller1/Documents/SAP/SAP GUI/{}.xlsx".format(f))
        for row in wb.sheets.active.range("A2:C2").expand("down").value:
            order, part, qty = row
            cohv[order] = [part, qty]
        wb.close()

    wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\mb51.xlsx")
    mb51 = wb.sheets.active.range("A2:Q2").expand("down").value
    wb.close()

    return mb51, cohv


def get_matl_data(orig_mb51, cohv, matl):
    mb51 = list()
    for row in orig_mb51:
        if row[2] != matl:
            continue

        order = row[8]
        qty = -1 * float(row[6])
        mtype = int(row[1])
        program = row[15]
        job = row[7]
        cc = row[16]

        if program:
            program = program.replace("#", "")

        if job:
            job = job.split("-")[1]
        elif cc:
            job = cc

        if mtype not in (201, 202, 221, 222, 261, 262):
            print("unmatched movement type:", mtype)

        if mtype in (261, 262) and order in cohv:
            assert order is not None, "Received no order for row ({})".format(row)
            # assert order in cohv, "MB51 has an order not in cohv: {}".format(order)
            part, pqty = cohv[order]
        else:
            part, pqty = None, None

        mb51.append([mtype, part, pqty, qty, program, job])

    
    con = sqlite3.connect(r"C:\temp\sapprod.db")
    cur = con.cursor()
    cur.execute("""
        SELECT part, qty, area, program, ArcDateTime
        FROM sigmanest
        WHERE matl=?
    """, [matl])
    burned = list()
    for part, qty, area, program, adt in cur.fetchall():
        burned.append([part, qty, area, program, datetime.strptime(adt, "%Y-%m-%d %H:%M:%S")])


    return mb51, burned


def reversals(mb51):
    res = list()

    revers = list()
    not_rev = list()
    for row in mb51:
        mtype, part, pqty, mqty, program, job = row
        if mtype in (202, 222, 262):
            if any([part, program]):
                revers.append([part, program, mqty])
        else:
            not_rev.append(row)

    for row in not_rev:
        mtype, part, pqty, mqty, program, job = row
        for i, r in enumerate(revers):
            if r == [part, program, -1 * mqty]:
                revers.pop(i)
                break
        else:
            res.append(row)

    # if len(revers) > 0:
    #     print(revers)
    #     print("Not matched reversals")
    #     print(tabulate(revers, headers="Order,Program,Qty".split(","), floatfmt=",.3f"))

    return res

def try_both(mb51, sn, matl):
    # mb51 row is [mtype, part, pqty, mqty, program, job]
    # sn row is   [part, qty, area, program, datetime]
    consumption = defaultdict(int)
    consumption_area = 0.0
    issue = list()
    issue_progs = list()
    issue_list = defaultdict(float)
    for mtype, part, pqty, mqty, program, job in mb51:
        if mtype == 261:
            consumption[part] += pqty
            consumption_area += mqty
        else:
            assert mtype in (201, 221)
            issue_progs.append(program)
            issue.append([mqty, program])
            issue_list[job] += mqty

    temp1, temp2 = list(), list()
    for row in sn:
        part, qty, area, program, dt = row
        
        if dt < start:
            continue

        if program in issue_progs or part not in consumption:
            temp1.append(row[:-1])
            continue

        if consumption[part] >= qty:
            consumption[part] -= qty
            consumption_area -= area
        else:
            if consumption[part] > 0:
                area_ea = area / qty
                qty -= consumption[part]
                area -= (qty * area_ea)

                consumption[part] = 0
                consumption_area -= qty * area_ea

            temp1.append([part, qty, area, program])

    for row in temp1:
        part, qty, area, program = row

        if part not in consumption:
            temp2.append(row)
            continue


        if consumption[part] >= qty:
            consumption[part] -= qty
            consumption_area -= area
        else:
            if consumption[part] > 0:
                area_ea = area / qty
                qty -= consumption[part]
                area -= (qty * area_ea)

                consumption[part] = 0
                consumption_area -= qty * area_ea

            temp2.append([part, qty, area, program])


    data = sorted(issue_list.items())
    print(tabulate(data))
    print("unmatched consumpion for {}: {}".format(matl, consumption_area))

    wb = xlwings.books.active
    template = wb.sheets["Template"]
    sheet = template.copy(name=matl.replace("/", "_"))
    sheet.range("G2").value = list(data)
    sheet.range("B2").value = list(sorted(temp2))



def print_results(data):
    with open('output.txt', 'w') as o:
        o.write("\n".join([x[0] for x in data]))
    print(tabulate(data, floatfmt=",.3f"))


if __name__ == "__main__":
    main()
