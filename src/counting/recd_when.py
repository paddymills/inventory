
from lib import sndb
import xlwings


def mm_recd(mm):
    """
    
    gets all plates from SimTrans log
    that match a given material master
    and dumps it to a new excel workbook.

    """


    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT *
            FROM Stock
            INNER JOIN TransActLog
                ON Stock.SheetName=TransActLog.ItemName
            WHERE TransActLog.PrimeCode = ?
        """, mm)

        data = [['Date Received', 'SheetName', 'Location']]
        for row in cursor.fetchall():
            data.append([
                row.DateReceived,
                row.SheetName,
                row.Location,
            ])

        wb = xlwings.Book()
        wb.sheets.active.range("A1").value = data


if __name__ == "__main__":
    mm_recd('50/50W-0008')
