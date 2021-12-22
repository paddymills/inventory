
from lib import sndb
from lib import printer


def get_burned(as_csv=False):
    with sndb.SndbConnection() as db:
        db.cursor.execute("""
            SELECT TOP 10
                CompletedDateTime AS Timestamp,
                ProgramName AS Program,
                SheetName AS Sheet,
                MachineName AS Machine
            FROM CompletedProgram
            WHERE MachineName NOT LIKE 'Plant_3_%'
            ORDER BY CompletedDateTime DESC
        """)

        res = db.collect_table_data()
        printer.print_to_source(res, as_csv)


if __name__ == "__main__":
    get_burned(as_csv=True)
