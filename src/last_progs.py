
from lib import sndb
from lib import printer
from tabulate import tabulate


def get_burned(as_csv=False):
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT TOP 10
                CompletedDateTime AS Timestamp,
                ProgramName AS Program,
                SheetName AS Sheet,
                MachineName AS Machine
            FROM CompletedProgram
            WHERE MachineName NOT LIKE 'Plant_3_%'
            ORDER BY CompletedDateTime DESC
        """)

        res=sndb.collect_table_data(cursor)
        printer.print_to_source(res, as_csv)


if __name__ == "__main__":
    get_burned(as_csv=True)
