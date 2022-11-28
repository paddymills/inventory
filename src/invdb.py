
from argparse import ArgumentParser
from collections import defaultdict
import csv
from datetime import datetime
from halo import Halo
from itertools import groupby
import os
import re
import sqlite3
from tabulate import tabulate
from tqdm import tqdm
from types import SimpleNamespace
import xlwings

from lib import paths
from lib.parsers import ParsedCnfRow, ParsedIssueRow
from lib.db import SndbConnection

sap_archive = os.path.join(paths.SAP_SIGMANEST_PRD, "Archive")
processed = os.path.join(paths.SAP_DATA_FILES, "processed")
originals = os.path.join(paths.SAP_DATA_FILES, "original")
prod_pattern = re.compile(r"Production_(\d{14}).(?:outbound.archive|ready)")
issue_pattern = re.compile(r"Issue_(\d{14}).(?:outbound.archive|ready)")

filename_datetime = "%Y%m%d%H%M%S"
job_pattern = re.compile(r"\d{7}\w-")

ipart = SimpleNamespace(matl=0, qty=4, wbs=2, plant=11, job=1, program=12)
imatl = SimpleNamespace(matl=6, qty=8, loc=10, wbs=7, plant=11)

class InventoryDb:

    def __init__(self):
        self.con = sqlite3.connect(r"C:\temp\sapprod.db")
        self.cur = self.con.cursor()

    def cleardb(self):
        self.cur.execute("DELETE FROM sap_archive")
        self.cur.execute("DELETE FROM issue")
        self.cur.execute("DELETE FROM original")
        self.cur.execute("DELETE FROM processed")
        self.cur.execute("DELETE FROM files")
        self.cur.execute("DELETE FROM sigmanest")
        self.con.commit()

    def readall(self):
        self.read_sap()
        self.read_processed()
        self.read_original()
        self.read_issue()

        self.read_sn()

    def read_dir(self, directory, filename_pattern, ParseType):
        base = os.path.basename(directory)

        self.progress = tqdm(os.scandir(directory), total=len(os.listdir(directory)))
        for f in self.progress:
            fn_match = filename_pattern.match(f.name)
            if f.is_file() and fn_match:
                self.progress.set_description("{}|{}".format(base, fn_match.group(1)))

                self.cur.execute("SELECT * FROM files WHERE filename=?", [f.path])
                if self.cur.fetchone() is None:
                    res = self.parse_file(f, ParseType)

                    if res:
                        yield res
      
    def parse_file(self, f, ParseType):
        inserts = list()
        with open(f.path) as pf:
            for i, line in enumerate(pf.readlines(), start=1):
                if line and line != "\n":
                    try:
                        split_and_ucase = line.upper().split("\t")
                        inserts.append( ParseType(split_and_ucase, ipart, imatl).db_input() + [f.path] )
                    except:
                        self.progress.write("{} needs fixed (line {})".format(f.name, i))
                        return

        self.cur.execute("""
            INSERT INTO files VALUES(?)
        """, [f.path])

        return inserts

    def read_sap(self):
        for res in self.read_dir(sap_archive, prod_pattern, ParsedCnfRow):
            self.cur.executemany("""
                INSERT INTO sap_archive(
                    part, job, part_wbs, part_qty,
                    matl, matl_wbs, matl_qty, matl_loc,
                    plant, program, filename)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, res)
            self.con.commit()
    
    def read_processed(self):
        for res in self.read_dir(processed, prod_pattern, ParsedCnfRow):
            self.cur.executemany("""
                INSERT INTO processed(
                    part, job, part_wbs, part_qty,
                    matl, matl_wbs, matl_qty, matl_loc,
                    plant, program, filename)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, res)
            self.con.commit()
    
    def read_issue(self):
        for res in self.read_dir(processed, issue_pattern, ParsedIssueRow):
            self.cur.executemany("""
                INSERT INTO issue(
                    code, user1, user2,
                    matl, wbs, qty, loc,
                    plant, program, filename)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, res)
            self.con.commit()
    
    def read_original(self):
        for res in self.read_dir(originals, prod_pattern, ParsedCnfRow):
            self.cur.executemany("""
                INSERT INTO original(
                    part, job, part_wbs, part_qty,
                    matl, matl_wbs, matl_qty, matl_loc,
                    plant, program, filename)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, res)
            self.con.commit()

    def read_sn(self):
        self.cur.execute("DELETE FROM sigmanest")
        self.con.commit()

        with SndbConnection() as db:
            cursor = db.cursor
            with Halo(text="reading sigmanest"):
                cursor.execute("""
                    SELECT
                        p.AutoID,
                        p.ArcDateTime,
                        REPLACE(p.PartName,'_', '-'),
                        p.QtyInProcess,
                        s.PrimeCode,
                        s.Mill,
                        s.Location,
                        p.QtyInProcess * (p.TrueArea + s.UsedArea * (s.ScrapFraction / q.Qty)) AS PartNestedArea,
                        p.ProgramName,
                        p.WONumber,
                        j.Job
                    FROM PIPArchive AS p
                        INNER JOIN StockArchive AS s
                            ON p.ProgramName=s.ProgramName
                            AND p.SheetName=s.SheetName
                        INNER JOIN (
                            SELECT SUM(QtyInProcess) AS Qty, ProgramName, SheetName
                            FROM PIPArchive
                            WHERE TransType='SN102'
                            GROUP BY SheetName, ProgramName
                        ) AS q
                            ON p.ProgramName=q.ProgramName
                            AND p.SheetName=q.SheetName
                        INNER JOIN (
                            SELECT PartName, Data1 as Job
                            FROM Part
                            WHERE PartName IN (SELECT PartName FROM PIPArchive)
                            UNION
                            SELECT PartName, Data1 as Job
                            FROM PartArchive
                            WHERE PartName IN (SELECT PartName FROM PIPArchive)
                        ) AS j
                            ON p.PartName=j.PartName
                    WHERE p.transtype='SN102'
                """)

            inserts = list()
            scan_pattern = re.compile(r"\d{3}\w-\w+-\w+-(\w+)")
            for row in tqdm(cursor.fetchall(), desc="reading sigmanest"):
                vals = list(row)
                job = vals.pop()
                match = scan_pattern.match(vals[2])
                if match and job is not None:
                    vals[2] = "{}-{}".format(job, match.group(1))

                inserts.append(vals)

            with Halo(text="uploading sigmanest"):
                self.cur.executemany("""
                    INSERT OR IGNORE INTO sigmanest(
                        AutoId, ArcDateTime,
                        part, qty, matl, wbs, loc, area, program, workorder
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, inserts)
                self.con.commit()
        

    def gen_prodfile(self, matl_pattern='%'):
        output = list()
        with open('tmp/invdb_input.csv') as c:
            reader = csv.DictReader(c)
            for row in reader:
                # print(row)
                if row['Id']:
                    self.cur.execute("""
                        SELECT
                            part, job, part_wbs, part_qty,
                            matl, matl_wbs, matl_qty, matl_loc,
                            plant, program, filename
                        FROM original
                        WHERE
                            id=?
                    """, [row['Id']])
                elif not row['Program']:
                    self.cur.execute("""
                        SELECT
                            part, job, part_wbs, part_qty,
                            matl, matl_wbs, matl_qty, matl_loc,
                            plant, program, filename
                        FROM original
                        WHERE
                            part=?
                        AND
                            matl like ?
                    """, [row['Part'], matl_pattern])
                else:
                    self.cur.execute("""
                        SELECT
                            part, job, part_wbs, part_qty,
                            matl, matl_wbs, matl_qty, matl_loc,
                            plant, program, filename
                        FROM original
                        WHERE
                            part=?
                        AND
                            program=?
                    """, [row['Part'], row['Program']])
                res = self.cur.fetchone()

                if res is None:
                    print("Nothing in database for", row['Part'])
                else:
                    output.append( ParsedCnfRow(res).ls(qty=int(row['Qty']), wbs=row['Wbs']) )

        if output:
            ts = datetime.now().strftime(filename_datetime)
            with open("Production_{}.ready".format(ts), 'w') as p:
                for line in output:
                    p.write("\t".join(line))

        return output

    def cmp_cohv(self):
        wb = xlwings.books.active
        data = wb.sheets.active.range("A1").expand().value[1:]

        ipart = 1
        iqty = 2

        mapping = defaultdict(int)
        for row in data:
            mapping[row[ipart]] += int(row[iqty])

        for k, v in mapping.items():
            self.cur.execute("""
                SELECT
                    sum(qty)
                FROM sigmanest
                WHERE part=?
            """, [k])
            qty, = self.cur.fetchone()
            if qty and qty > v:
                print("{}: {}/{}".format(k, v, qty))

    def cmp_cohv_stock(self):
        wb = xlwings.books.active
        data = wb.sheets["ForCompare"].range("A1").expand().value[1:]

        imatl = 2
        imqty = 6
        ipart = 9
        iqty = 10

        keyfunc = lambda x: x[0]
        cleaned_data = sorted([(r[imatl], r[ipart], r[iqty], abs(r[imqty])) for r in data], key=keyfunc)
        grouped = groupby(cleaned_data, key=keyfunc)
        final = list()
        totals = list()
        for mm, _parts in grouped:
            parts = dict()
            for _, part, qty, consumed in _parts:
                if part not in parts:
                    parts[part] = [0, 0]
                parts[part][0] += qty
                parts[part][1] += consumed

            self.cur.execute("""
                SELECT
                    part, qty, area
                FROM sigmanest
                WHERE matl=?
                AND ArcDateTime > '2022-01-01'
            """, [mm])
            for p, q, a in self.cur.fetchall():
                if p in parts:
                    parts[p][0] -= q
                    parts[p][1] -= q * a
                # else:
                #     print("Part not in SAP:", p, q, a)
            
            print("values for", mm)
            tab = list()
            totals.append([mm, 0.0])
            for k, v in parts.items():
                if v[0] != 0:
                    tab.append([k, *v])
                    final.append([mm, k, *v])
                    totals[-1][1] += v[1]
            # print(tabulate(tab))

        print(tabulate(sorted(final)))
        print(tabulate(sorted(totals), floatfmt=",.3f"))

    def gen_mm_parts(self, inputfile=None, mm=None):
        if not inputfile:
            inputfile = "tmp/stock.list"
        if mm is None:
            mm = list()
            with open(inputfile) as s:
                mm.extend(s.read().split("\n"))
        
        parts = list()
        for matl in mm:
            self.cur.execute("""
                SELECT DISTINCT
                    part
                FROM sigmanest
                WHERE matl=?
                AND ArcDateTime > '2022-01-01'
            """, [matl])
            for part, in self.cur.fetchall():
                if job_pattern.match(part):
                    parts.append(part)

        with open('tmp/stockparts.txt', 'w') as s:
            s.write("\n".join(sorted(parts)))

    def mm_parts(self):
        mm = list()
        with open("tmp/stock.txt") as s:
            mm.extend(s.read().split("\n"))
        
        wb = xlwings.books.active
        orders = wb.sheets["MB51_all"].range("I2").expand("down").value
        material = wb.sheets["MB51_all"].range("C2").expand("down").value
        cohv = wb.sheets["COHV_all"].range("A2").expand().value
        
        rows = list()
        compare = { k: defaultdict(int) for k in mm }
        for row in cohv:
            try:
                matl = material[orders.index(row[0])]
                r = [matl, *row[:3]]
                rows.append(r)
                compare[matl][row[1]] += row[2]
            except ValueError:
                pass

        wb.sheets["ForCompareAll"].range("A2").value = rows
        lines = [["Material", "Part", "SAP", "Sigmanest", "Diff", "DiffArea"]]
        for matl, vals in compare.items():
            for part, qty in tqdm(vals.items(), desc=matl):
                line = [matl, part, int(qty), 0, 0, 0.0]
                self.cur.execute("""
                    SELECT DISTINCT
                        qty, area
                    FROM sigmanest
                    WHERE part=?
                    AND matl=?
                """, [part, matl])

                total_area = 0.0
                num = 0
                for q, area in self.cur.fetchall():
                    line[3] += q
                    total_area += area
                    num += 1

                if num > 0:
                    line[4] = line[2] - line[3]
                    line[5] = line[4] * ( total_area / num )
                    if line[4]  != 0:
                        lines.append(line)

        print(tabulate(lines, headers="firstrow"))
        with open('tmp/stockcmp.csv', 'w', newline='') as c:
            writer = csv.writer(c)
            for row in lines:
                writer.writerow(row)

    def test(self, material):
        wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\mb51.xlsx")
        mb51 = wb.sheets.active.range("A2").expand().value
        wb.close()

        wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\cohv.xlsx")
        cohv = wb.sheets.active.range("A2").expand().value
        wb.close()

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
        mdiffs = [
            27640.286,
            132751.939,
            203178.407,
            175435.849,
            400566.586,
            125369.564,
            569176.200,
            37430.646,
            16223.344,
            129230.919,
        ]
        if material != "all":
            if material in materials:
                mdiffs = [mdiffs[materials.index(material)]]
            else:
                mdiffs = [0]
            materials = [material]

        orders = dict()
        qcnf = defaultdict(int)
        issue_progs = list()
        for row in cohv:
            order, part, qty = row[:3]
            orders[order] = part
            qcnf[part] +=  int(qty)

        
        diff_area = defaultdict(lambda: [0 for _ in materials])
        missing = defaultdict(lambda: [0 for _ in materials])
        for i, mm in enumerate(materials):

            qtycons = defaultdict(float)
            for row in mb51:
                if row[2] != mm:
                    continue

                order = row[8]
                transact = row[1]
                reference = row[16]
                if transact in ('261', '262'):
                    if order in orders:
                        # matl = row[2]
                        qty = -1 * float(row[6])

                        qtycons[orders[order]] += qty
                elif transact in ('221', '222') and reference:
                    issue_progs.append(reference.replace("#", ""))

            qty_per = dict()
            tab = ["Part,Sap,Sn,Diff,DiffArea".split(",")]
            for part, qty in qtycons.items():
                qty_per[part] = qty / qcnf[part]

                self.cur.execute("""
                        SELECT DISTINCT
                            qty, program
                        FROM sigmanest
                        WHERE part=?
                        AND matl=?
                    """, [part, mm])

                total = 0
                for q, prog in self.cur.fetchall():
                    if prog in issue_progs:
                        continue
                    total += q

                sap = qcnf[part]
                diff = total - sap
                if diff > 0:
                    diff_per = diff * qty_per[part]
                    tab.append([part, sap, total, diff, diff_per])
                    diff_area[part.split("-")[0]][i] += diff_per

            # try without part constraint
            self.cur.execute("""
                    SELECT DISTINCT
                        part, qty, program, area
                    FROM sigmanest
                    WHERE matl=?
                    AND ArcDateTime > '2020-01-01'
                    AND part NOT LIKE '%-%-%-%'
                """, [mm])
            for part, q, prog, area in self.cur.fetchall():
                if part not in qtycons and prog not in issue_progs:
                    if not job_pattern.match(part):
                        job = part.split("-")[0]
                        missing[job][i] += area
                        # missing[part][i] += area

            if len(materials) == 1:
                print(tabulate(tab, headers="firstrow", tablefmt="simple"))
                print("\n" + "-" * 50 + "\n")

        # parts
        data = [["Job"] + materials]
        # data.extend([[k, *v] for k, v in diff_area.items()])
        for k, v in sorted(diff_area.items()):
            data.append([k, *[x if x>0 else None for x in v]])
        data.append("\001")   # separating line
        totals = [["parts"] + [0 for _ in materials]]
        for r in diff_area.values():
            for i, c in enumerate(r[1:], start=1):
                totals[-1][i] += c

        # non production
        # data.extend([k, *v] for k, v in missing.items())
        for k, v in sorted(missing.items()):
            data.append([k, *[x if x>0 else None for x in v]])
        data.append("\001")   # separating line
        totals.append(["non-prod"] + [0 for _ in materials])
        for r in missing.values():
            for i, c in enumerate(r, start=1):
                totals[-1][i] += c

        totals.append(["CountDiff"] + mdiffs)
        data.extend(totals)
        data.append("\001")   # separating line

        print(tabulate(data, headers="firstrow", floatfmt=".3f"))

    def test2(self, material):
        wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\mb51.xlsx")
        mb51 = wb.sheets.active.range("A2").expand().value
        wb.close()

        wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\cohv.xlsx")
        cohv = wb.sheets.active.range("A2").expand().value
        wb.close()

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
        mdiffs = [
            27640.286,
            132751.939,
            203178.407,
            175435.849,
            400566.586,
            125369.564,
            569176.200,
            37430.646,
            16223.344,
            129230.919,
        ]
        if material != "all":
            if material in materials:
                mdiffs = [mdiffs[materials.index(material)]]
            else:
                mdiffs = [0]
            materials = [material]

        orders = dict()
        for row in cohv:
            order, part = row[:2]
            orders[order] = part


        for i, mm in enumerate(materials):

            consumption = list()
            issue = defaultdict(float)

            for row in mb51:
                if row[2] != mm:
                    continue

                order = row[8]
                transact = row[1]
                reference = row[16]
                qty = -1 * float(row[6])
                if transact in ('261', '262'):
                    if order in orders:
                        consumption.append([orders[order], qty])
                elif transact in ('221', '222') and reference:
                    prog = reference.replace("#", "")
                    issue[prog] += qty

            self.cur.execute("""
                SELECT DISTINCT
                    part, part_qty, matl_qty, program, filename
                FROM original
                WHERE matl=?
            """, [mm])
            no_match = list()
            for part, qty, area, prog, fn in self.cur.fetchall():

                for i, row in enumerate(consumption):
                    p, q = row
                    if part == p and qty == q:
                        consumption.remove(i)
                        break
                else:
                    if prog in issue:
                        issue[prog] -= qty
                    else:
                        dt = datetime.strptime(prod_pattern.match(os.path.basename(fn)).group(1), filename_datetime)
                        if datetime(2022, 1, 1) <= dt:
                            no_match.append([part, qty, area, prog])

            still_no_match = list()
            for part, qty, area, prog in no_match:
                to_del = list()
                for i, row in enumerate(consumption):
                    p, q = row
                    if p == part:
                        if qty <= q:
                            q -= qty
                        else:
                            q = 0
                            qty -= q

                        if q == 0:
                            to_del.append(i)

                    if qty == 0:
                        break

                for i in to_del:
                    consumption.remove(i)

                if qty > 0:
                    still_no_match.append([part, qty, area, prog])

            print(tabulate(consumption, headers=["part", "area"]))
            print(tabulate([[k, v] for k, v in issue.items() if v > 0]))
            print(tabulate(still_no_match))
            
    def notcnf(self):
        wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\export.xlsx")
        cohv = wb.sheets.active.range("A2").expand().value

        confirmed = defaultdict(int)
        planned = defaultdict(list)

        for row in cohv:
            part = row[1]
            ordertype = row[5]
            qty = row[2]
            wbs = row[3]

            if ordertype == "PP01": # confirmed
                confirmed[part] += qty
            elif ordertype == "PR":  # planned
                planned[part].append([qty, wbs])

        parts = list(set([*confirmed.keys(), *planned.keys()]))
        to_cnf = list()
        for part in parts:
            self.cur.execute("""
                SELECT DISTINCT
                    sum(part_qty)
                FROM original
                WHERE part=?
            """, [part])
            qty, = self.cur.fetchone()
            if qty is None:
                qty = 0
            qty -= confirmed[part]

            pl_ords = sorted(planned[part], reverse=True)
            while qty > 0 and len(pl_ords) > 0:
                cnf_qty, wbs = pl_ords.pop(0)
                if cnf_qty > qty:
                    cnf_qty = qty
                    qty = 0
                else:
                    qty -= cnf_qty

                to_cnf.append([part, cnf_qty, wbs])

        with open('tmp/invdb_input.csv', 'w', newline='') as c:
            writr = csv.writer(c)
            writr.writerow('Part,Qty,Wbs,Program,Id'.split(','))
            for part, qty, wbs in to_cnf:
                writr.writerow([part, int(qty), wbs, None, None])

        materials = {
            "50/50W-0004":          [  27640.286, 0.0 ],
            "50/50W-0006":          [ 132751.939, 0.0 ],
            "50/50W-0008":          [ 203178.407, 0.0 ],
            "50/50W-0010":          [ 175435.849, 0.0 ],
            "50/50W-0012":          [ 400566.586, 0.0 ],
            "50/50W-0014":          [ 125369.564, 0.0 ],
            "50/50W-0100":          [ 569176.200, 0.0 ],
            "50/50W-0104":          [  37430.646, 0.0 ],
            "50/50W-0106":          [  16223.344, 0.0 ],
            "50/50W-0108":          [ 129230.919, 0.0 ],

            
            # "9-50W-0004":           [   2927.602, 0.0 ],
            # "9-50-0300":            [    686.587, 0.0 ],
            # "9-50T2-0312B":         [    379.435, 0.0 ],
            # "9-50-0112":            [  12767.125, 0.0 ],
            # "9-50-0100":            [   2162.920, 0.0 ],
            # "9-HPS50W-PL0012":      [      0.256, 0.0 ],
            # "9-HPS50W-PL0100":      [      0.899, 0.0 ],
            # "9-HPS50W-PL0104":      [   2357.422, 0.0 ],
            # "9-HPS50WF3-0114":      [      0.000, 0.0 ],
            # "9-HPS50WT3-0114":      [      0.141, 0.0 ],
            # "9-HPS50WT3-0200A":     [   3041.918, 0.0 ],
            # "9-50F2-0012":          [   5977.431, 0.0 ],
            # "9-50W-0106":           [   7597.947, 0.0 ],
            # "9-50W-0108":           [      0.007, 0.0 ],
            # "9-50W-0110":           [   1000.487, 0.0 ],
            # "9-50-0112":            [      0.000, 0.0 ],
            # "9-HPS50WF3-0108":      [   7028.437, 0.0 ],
            # "9-50-0108":            [  43025.345, 0.0 ],
        }

        key = list(materials.keys())[0].split("-")[0] + "-%"
        written = self.gen_prodfile(matl_pattern=key)


        for row in written:
            parsed = ParsedCnfRow(row, ipart, imatl)
            if parsed.matl_master in materials:
                materials[parsed.matl_master][1] += parsed.matl_qty


        data = []
        for k, v in materials.items():
            data.append([k, *v, v[0] - v[1]])
        print(tabulate(sorted(data), headers="Material,Variance,ProdFile,Diff".split(","), floatfmt=",.3f"))

    def notissued(self):
        mm = list()
        with open("tmp/stock.txt") as s:
            mm.extend(s.read().split("\n"))

        wb = xlwings.Book(r"C:\Users\PMiller1\Documents\SAP\SAP GUI\mb51.xlsx")
        mb51 = wb.sheets.active.range("A2").expand().value

        self.cur.execute("""
            SELECT area, matl, program
            FROM sigmanest
            WHERE isprod=0
            AND ArcDateTime > '2022-01-01'
        """)
        sn = defaultdict(lambda: defaultdict(float))
        for area, matl, prog in self.cur.fetchall():
            if matl in mm:
                sn[matl][prog[0]] += area

        sap = defaultdict(float)
        for row in mb51:
            matl = row[2]
            transact = row[1]
            reference = row[16]
            qty = -1 * float(row[6])

            if transact not in ('221', '222'):
                continue

            if reference:
                reference = reference.replace("#", "")
                if reference in sn[matl]:
                    del sn[matl][prog]
                    continue

            sap[matl] += qty


        sn = {m: sum(v.values()) for m, v in sn.items()}

        output = list()
        output.append(["Material", "SAP", "SN", "Diff"])
        for k in sorted(set([*sap, *sn])):
            output.append([k, sap[k], sn[k], sap[k] - sn[k]])

        print(tabulate(output, headers="firstrow", floatfmt=",.3f"))
                    

if __name__ == "__main__":
    inv = InventoryDb()

    ap = ArgumentParser()
    ap.add_argument("-c", "--clear",        action="store_true",    help="❗ Delete all data ❗")
    ap.add_argument("-r", "--read",         action="store_true",    help="Read files and sigmanest into db")
    ap.add_argument(      "--readsn",       action="store_true",    help="Read sigmanest into db")
    ap.add_argument("-g", "--generate",     action="store_true",    help="Generate output (tmp/invdb_input.csv -> Production_*.ready")
    ap.add_argument("-m", "--move",         action="store_true",    help="move Production_*.ready")
    ap.add_argument("-n", "--notcnf",       action="store_true",    help="Calculate not confirmed")
    ap.add_argument("-i", "--notissued",    action="store_true",    help="Calculate not issued")
    ap.add_argument(      "--cohv",         action="store_true",    help="compare COHV to sigmanest")
    ap.add_argument(      "--cohv-stock",   action="store_true",    help="compare COHV for stock to sigmanest")
    ap.add_argument("-s", "--stockmm",      action="store_true",    help="generate stock MM parts list")
    ap.add_argument("-p", "--processmm",    action="store_true",    help="process stock MM parts list")
    ap.add_argument("-t", "--test",                                 help="test process")
    ap.add_argument(      "--test2",                                help="test process2")

    ap.add_argument(      "--file",                                 help="file input")
    ap.add_argument(      "--materials",    nargs="*",              help="materials to query")
    args = ap.parse_args()

    if args.clear:
        inv.cleardb()
    if args.read:
        inv.readall()
    if args.readsn:
        inv.read_sn()

    if args.generate:
        inv.gen_prodfile()

    if args.move:
        os.system(r"pwsh -NoProfile -Command mv Production_*.ready \\hiifileserv1\sigmanestprd\Outbound")
    if args.notcnf:
        inv.notcnf()
    if args.notissued:
        inv.notissued()

    if args.cohv:
        inv.cmp_cohv()
    if args.cohv_stock:
        inv.cmp_cohv_stock()
    if args.stockmm:
        inv.gen_mm_parts(inputfile=args.file, mm=args.materials)
    if args.processmm:
        inv.mm_parts()

    if args.test:
        inv.test(args.test)

    if args.test2:
        inv.test2(args.test2)
