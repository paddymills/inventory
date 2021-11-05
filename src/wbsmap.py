
import sndb
from tabulate import tabulate


def main():
    part = input("Part: ")
    if not part:
        return

    show_part(part)

def show_part(part):
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()

        data = [None]
        cursor.execute("""
            SELECT * FROM SAPPartWBS
            WHERE PartName=?
        """, part)

        for row in cursor.fetchall():
            data.append(row)
        else:
            data[0] = [t[0] for t in row.cursor_description]

    print(tabulate(data, headers="firstrow"))


if __name__ == "__main__":
    # show_part("1200055B-X206F")
    main()
