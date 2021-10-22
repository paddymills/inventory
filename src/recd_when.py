
from lib import sndb
import xlwings

with sndb.get_sndb_conn() as db:
    cursor = db.cursor()
    cursor.execute("""
        SELECT *
        FROM Stock
        INNER JOIN TransActLog
            ON Stock.SheetName=TransActLog.ItemName
        WHERE TransActLog.PrimeCode = '50/50W-0008'
    """)

    data = [['Date Received', 'SheetName', 'Location']]
    for row in cursor.fetchall():
        data.append([
            row.DateReceived,
            row.SheetName,
            row.Location,
        ])

    wb = xlwings.Book()
    wb.sheets.active.range("A1").value = data
