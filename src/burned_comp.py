
from lib import sndb, printer


def get_burned(csv=False):
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT
                CompletedProgram.CompletedDateTime AS ArcDateTime,
                CompletedProgram.ProgramName AS Program,
                CompletedProgram.MachineName AS Machine,
                Program.SheetName AS Sheet,
                Stock.Location AS Location,
                Stock.PrimeCode AS SAP_MM
            FROM CompletedProgram
            INNER JOIN Program
                ON  CompletedProgram.ProgramName=Program.ProgramName
                AND CompletedProgram.SheetName=Program.SheetName
            INNER JOIN Stock
                ON Program.SheetName=Stock.SheetName
            WHERE CompletedProgram.ProgramName IN (
                SELECT ProgramName
                FROM TransAct
                WHERE TransType = 'SN70'
            )
            AND CompletedProgram.MachineName NOT LIKE 'Plant_3_%'
            ORDER BY Stock.PrimeCode
        """)

        res = sndb.collect_table_data(cursor, "Part")
        printer.print_to_source(res, csv)


if __name__ == "__main__":
    get_burned(csv=True)
