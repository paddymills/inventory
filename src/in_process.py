
from lib import sndb

with sndb.get_sndb_conn() as db:
    cursor = db.cursor()
    cursor.execute("""
        SELECT
            CompletedProgram.AutoID, CompletedProgram.CompletedDateTime,
            CompletedProgram.ProgramName,
            Program.SheetName, Stock.PrimeCode
        FROM CompletedProgram
        INNER JOIN Program
            ON CompletedProgram.ProgramName=Program.ProgramName
        INNER JOIN Stock
            ON Program.SheetName=Stock.SheetName
        WHERE CompletedDateTime > '2021-10-18 04:00:00'
    """)

    for row in cursor.fetchall():
        dt = row.CompletedDateTime.isoformat()
        print('[{}] {} | {}\t{}\t{}'.format(dt, row.AutoID, row.ProgramName, row.SheetName, row.PrimeCode))
