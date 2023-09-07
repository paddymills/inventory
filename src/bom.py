
import subprocess
from lib import db
from lib import part

import csv
import os
import pretty_errors
import xlwings

from colorama import Fore, Style
from tabulate import tabulate
from argparse import ArgumentParser

XML_PARTS_XLS = r"\\HSSFILESERV1\HSSshared\HSSI Lean\Job Plans\Pre-Nesting Tools\XML PRENESTING\XML-Parts.xls"

pretty_errors.configure(
    line_number_first   = True,
    display_link        = True,
)

def main():
    parser = ArgumentParser()
    parser.add_argument("--all", action="store_const", const="all", default='secondary', help="skip main member")
    parser.add_argument("--csv", action="store_true", help="Return as csv output")
    parser.add_argument("--check", action="store_true", help="Check if jobs in check.csv have data")
    parser.add_argument("--commit-loop", action="store_true", help="Loop to generate from positive selections in check.csv")
    parser.add_argument("--no-stock", action="store_true", help="do not skip stock plate")
    parser.add_argument("--fix-workorder", action="store_true", help="fix work order quantities")
    # parser.add_argument("--secondary", action="store_const", const="secondary", default='all', help="skip main member")
    parser.add_argument("--sort", action="extend", nargs="+", type=str, help="Columns to sort by")
    parser.add_argument("--addlcols", action="extend", nargs="+", type=str, help="Additional columns to show")
    parser.add_argument("--verbose", "-v", action="count", default=0, help="set verbosity level")
    parser.add_argument("job", nargs="?", default=None)
    parser.add_argument("shipment", nargs="?", default=None)
    args = parser.parse_args()

    # set directory to script root
    os.chdir(os.path.dirname(__file__))

    if args.verbose > 1:
        print(args)

    if args.check:
        return check(args)
    else:
        if "-" in args.job:
            assert args.shipment is None, "Shipment must be specified in either job or shipment arguments"
            args.job, args.shipment = args.job.split("-")

        if args.fix_workorder:
            args.no_stock = True

        process(args)

def process(args):
    parts = list()
    to_print = list()
    with db.DbConnection(server='HSSSQLSERV', use_win_auth=True) as conn:
        conn.cursor.execute(
            "EXEC BOM.SAP.GetBOMData @Job=?, @Ship=?",
            args.job, args.shipment
        )

        for r in conn.cursor.fetchall():
            # print(r)
            p = part.Part(r)

            if p.for_prenest(args.all, args.no_stock):
                parts.append(p)
                if args.verbose == 1 or args.sort or args.addlcols:
                    to_print.append(p)

            elif args.verbose > 1:
                to_print.append(p)

    if args.sort:
        to_print.sort(key=lambda x: [getattr(x, c) for c in args.sort])
    if to_print:
        if args.addlcols:
            pr = list()
            for p in to_print:
                addl_cols = [getattr(p, c) for c in args.addlcols]
                pr.append([p.mark, p.size, p.item, *addl_cols])
            print(tabulate(pr, headers=["Mark", "Size", "Item", *map(lambda x: x.capitalize(),args.addlcols)]))
        else:
            print(tabulate([(p.mark, p.desc, p.item) for p in to_print], headers=["Mark", "Size", "Item"]))

    if args.fix_workorder:
        fix_workorder(args.job, args.shipment, parts)
    else:
        dump_to_xl(parts, args.job, args.shipment)


def check(args):
    with db.DbConnection(server='HSSSQLSERV', use_win_auth=True) as conn:
        with open("check.csv") as checkfile:
            reader = csv.DictReader(checkfile)
            for row in reader:
                job, shipment = row['Job'], int(row['Shipment'])

                conn.cursor.execute(
                    "EXEC BOM.SAP.GetBOMData @Job=?, @Ship=?",
                    job, shipment
                )

                if any([part.Part(r).for_prenest(args.all, args.no_stock) for r in conn.cursor.fetchall()]):
                    print("{}✔️ {}-{} has data now".format(Fore.GREEN, job, shipment), end = '|')
                    # commit loop: process
                    #   - set job and shipment in args
                    #   - wait for user input to continue
                    if args.commit_loop:
                        args.job = job
                        args.shipment = shipment
                        process(args)
                        input(" ⇢ press any key to continue")
                    else:
                        print();
                else:
                    print("{}❌{}-{} has no data".format(Fore.RED, job, shipment))

    if args.commit_loop:
        opt = input("Run xml import? [y/N] ")
        if opt and opt[0].upper() == 'Y':
            subprocess.run([r"\\HSSIENG\SNDataPrd\__oys\bin\SNImportXML\SNImportXML.exe", "PROCESSXML"])


def fix_workorder(job, shipment, parts):
    wo_pattern = "{}-{}-%".format(job, shipment)
    with db.SndbConnection() as conn:
        for p in parts:
            sn_part = "{}_{}".format(job, p.mark)
            conn.cursor.execute(
                "SELECT * FROM Part WHERE PartName=? AND WONumber LIKE ?",
                (sn_part, wo_pattern)
            )
            if conn.cursor.fetchone() is None:
                conn.cursor.execute(
                    """
                        INSERT INTO TransAct (
                            TransType, District, OrderNo, ItemName, Qty, DwgNumber, Material,
                            Remark, ItemData1, ItemData2, ItemData5, ItemData6, ItemData7, ItemData8, ItemData9, ItemData12, ItemData14
                        )
                        SELECT
                            'SN84' AS TransType,
                            2 AS District,
                            REPLACE(WONumber, CONCAT('-', Data2, '-'), CONCAT('-', ?, '-')) AS WONumber,
                            PartName,
                            ? AS QtyOrdered,
                            DrawingNumber,
                            Material,
                            Remark,
                            ? AS Data1,
                            ? AS Data2,
                            (
                                SELECT TOP 1 Data5 AS ChargeRef
                                FROM Part
                                WHERE WONumber LIKE ?
                            ) AS Data5,
                            Data6, Data7, Data8,
                            Data9, Data12,
                            'HighHeatNum' AS Data14
                        FROM (
                            SELECT
                                WONumber, PartName, DrawingNumber, Material, Remark, Data2, Data6, Data7, Data8, Data9, Data12
                            FROM Part

                            UNION

                            SELECT
                                WONumber, PartName, DrawingNumber, Material, Remark, Data2, Data6, Data7, Data8, Data9, Data12
                            FROM PartArchive
                        ) AS Part
                        WHERE WONumber LIKE ? AND PartName=?
                    """,
                    (
                        shipment,
                        p.qty, job, shipment,
                        wo_pattern,
                        wo_pattern.replace("-%s-" % shipment, "-%-"),
                        sn_part
                    )
                )

            else:
                conn.cursor.execute(
                    """
                        UPDATE Part SET QtyOrdered=?
                        WHERE PartName=? AND WONumber LIKE ?
                    """,
                    (p.qty, sn_part, wo_pattern)
                )

        conn.commit()

def dump_to_xl(data, job, shipment):
    wb = xlwings.Book(XML_PARTS_XLS)
    wb.macro("clear")()

    to_prenest = list()
    for part in data:
        to_prenest.append(part.xml_format())

    wb.sheets("DATA").range("A2").value = to_prenest
    wb.sheets("DATA").range("JOB").value = job.upper()
    wb.sheets("DATA").range("SHIP").value = shipment


if __name__ == "__main__":
    main()
