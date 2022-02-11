
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
    parser.add_argument("--csv", action="store_true", help="Return as csv output")
    parser.add_argument("--secondary", action="store_const", const="secondary", default='all', help="skip main member")
    parser.add_argument("job", nargs="?", default=None)
    parser.add_argument("shipment", nargs="?", default=None)
    args = parser.parse_args()

    if "-" in args.job:
        assert args.shipment is None, "Shipment must be specified in either job or shipment arguments"
        args.job, args.shipment = args.job.split("-")

    parts = list()
    with db.DbConnection(server='HSSSQLSERV', use_win_auth=True) as conn:
        conn.cursor.execute(
            "EXEC BOM.SAP.GetBOMData @Job=?, @Ship=?",
            args.job, args.shipment
        )

        for r in conn.cursor.fetchall():
            p = part.Part(r)

            if p.for_prenest(args.secondary):
                parts.append(p)

            print(p)

    dump_to_xl(parts, args.job, args.shipment)


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
