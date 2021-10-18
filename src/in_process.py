
from lib import sndb

with sndb.get_sndb_conn() as db:
    cursor = db.cursor()
    cursor.execute("""
        SELECT *
        FROM CompletedProgram
        WHERE CompletedDateTime > '2021-10-18 04:00:00'
    """)

    for row in cursor.fetchall():
        dt = row.CompletedDateTime.isoformat()
        print('{} | {}\t{}\t{}'.format(row.AutoID, row.ProgramName, row.SheetName, dt))
