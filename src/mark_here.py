
from lib import sndb
import xlwings


def main():
    wb = xlwings.books.active
    sheet = wb.sheets.active

    sheet_col = sheet.range((1, 2)).expand('right').value.index('Sheet Name') + 2

    update_count = 0
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()

        for sheet_name in sheet.range((2, sheet_col)).expand('down').value:
            cursor.execute("""
                SELECT SheetName, Location, PrimeCode
                FROM Stock
                WHERE SheetName=?
            """, sheet_name)
            rows_found = 0
            for row in cursor.fetchall():
                rows_found += 1
                print("Updating {} ({} in {})".format(row.SheetName, row.Location, row.PrimeCode))

            assert rows_found == 1

            cursor.execute("""
                UPDATE Stock
                SET SheetData4='10.18.21'
                WHERE SheetName=?
            """, sheet_name)

            update_count += 1

        cursor.commit()

    print('\n\tMarked {} sheets as here\n'.format(update_count))


if __name__ == "__main__":
    main()
