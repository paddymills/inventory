
from lib import db
from lib import part
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

        for r in conn.cursor.fetchall():
            p = part.Part(r)
            if p.type != "PL":
                continue
            print(p)


if __name__ == "__main__":
    main()
