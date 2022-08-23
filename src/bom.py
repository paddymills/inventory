
from lib import db
from lib import part

import pretty_errors
import xlwings

from argparse import ArgumentParser

XML_PARTS_XLS = r"\\HSSFILESERV1\HSSshared\HSSI Lean\Job Plans\Pre-Nesting Tools\XML PRENESTING\XML-Parts.xls"

pretty_errors.configure(
    line_number_first   = True,
    display_link        = True,
)

def main():
    parser = ArgumentParser()
    parser.add_argument("--verbose", "-v", action="count", default=0, help="set verbosity level")
    parser.add_argument("--csv", action="store_true", help="Return as csv output")
    parser.add_argument("--all", action="store_const", const="all", default='secondary', help="skip main member")
    parser.add_argument("--no-stock", action="store_true", help="do not skip stock plate")
    parser.add_argument("--fix-workorder", action="store_true", help="fix work order quantities")
    # parser.add_argument("--secondary", action="store_const", const="secondary", default='all', help="skip main member")
    parser.add_argument("job", nargs="?", default=None)
    parser.add_argument("shipment", nargs="?", default=None)
    args = parser.parse_args()

    if "-" in args.job:
        assert args.shipment is None, "Shipment must be specified in either job or shipment arguments"
        args.job, args.shipment = args.job.split("-")

    if args.fix_workorder:
        args.no_stock = True

    if args.verbose > 1:
        print(args)

    parts = list()
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

            if args.verbose > 0:
                print(p)

    if args.fix_workorder:
        fix_workorder(args.job, args.shipment, parts)
    else:
        dump_to_xl(parts, args.job, args.shipment)


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
