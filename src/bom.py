
from lib import db
from lib import printer

with db.DbConnection(server='HSSSQLSERV', use_win_auth=True) as db:
    job = "1200055C"
    ship = 3
    db.cursor.execute("EXEC BOM.SAP.GetBOMData @Job=?, @Ship=?", job, ship)

    res = db.collect_table_data()
    printer.print_to_source(res) 
