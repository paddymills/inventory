
from lib import sndb
import xlwings


def main():
    nested_on_mm()


def nested_on_mm():

    mms = list()
    with open('mm.txt', 'r') as f:
        for line in f.readlines():
            mms.append(line.replace("\n", ""))

    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()

        parts = list()
        for mm in mms:
            if not mm:
                continue

            cursor.execute("""
                SELECT DISTINCT
                    PIP.PartName
                FROM PIPArchive AS PIP
                INNER JOIN StockHistory AS Stock
                    ON Stock.ProgramName=PIP.ProgramName
                WHERE   Stock.PrimeCode = ?
                AND     PIP.TransType = 'SN102'
                AND     PIP.ArcDateTime > '2020-01-01'
                AND     PIP.WONumber NOT IN ('EXTRAS', 'REMAKES')
            """, mm)

            for row in cursor.fetchall():
                parts.append(row.PartName.replace("_", "-"))

        parts = set(parts)

        with open("parts.txt", "w") as f:
            f.write("\n".join(parts))


if __name__ == "__main__":
    main()
