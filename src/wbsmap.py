
from argparse import ArgumentParser
from lib import sndb
from lib import printer


def main():
    while True:
        part = input("\n\tPart: ")
        if not part:
            return

        show_part(part)

def show_part(part):
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT * FROM SAPPartWBS
            WHERE PartName=?
        """, part)

        data = sndb.collect_table_data(cursor, "wbsmap")

    printer.print_to_source(data, as_csv=True)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("part", nargs="?", default=None)
    args = parser.parse_args()

    if args.part:
        show_part(args.part)
    else:
        # show_part("1200055B-X206F")
        main()
