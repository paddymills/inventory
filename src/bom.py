
from lib import db
from lib import printer

from argparse import ArgumentParser

def main():
    parser = ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Return as csv output")
    parser.add_argument("job", nargs="?", default=None)
    parser.add_argument("shipment", nargs="?", default=None)
    args = parser.parse_args()

    with db.DbConnection(server='HSSSQLSERV', use_win_auth=True) as conn:
        conn.cursor.execute(
            "EXEC BOM.SAP.GetBOMData @Job=?, @Ship=?",
            args.job, args.shipment
        )

        res = conn.collect_table_data()
        printer.print_to_source(res, as_csv=args.csv) 


if __name__ == "__main__":
    main()
