
from lib import sndb
from tabulate import tabulate


def main():
    get_burned()


def get_burned():
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT
                CompletedProgram.AutoID, CompletedProgram.CompletedDateTime,
                CompletedProgram.ProgramName, CompletedProgram.MachineName,
                Program.SheetName,
                Stock.Location, Stock.PrimeCode,
                Stock.SheetData4
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

        data = []
        for row in cursor.fetchall():
            data.append(dict(
                ArcDateTime=row.CompletedDateTime,
                Prog=row.ProgramName,
                Sheet=row.SheetName,
                Location=row.Location,
                SAP_MM=row.PrimeCode,
                Machine=row.MachineName,
                Counted=(row.SheetData4 == '10.18.21'),
            ))

        print(tabulate(data, headers="keys"))


if __name__ == "__main__":
    main()
