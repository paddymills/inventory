
from lib import parsers
from lib import sndb

from collections import defaultdict
from tqdm import tqdm

def main():
    data = parsers.parse_sheet()
    nested = defaultdict(list)

    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()

        for x in tqdm(data):
            cursor.execute("""
                SELECT
                    PIP.PartName AS part,
                    PIP.ProgramName AS prog,
                    PIP.QtyInProcess AS qty,
                    Program.MachineName as mach
                FROM PIP
                INNER JOIN Program
                    ON PIP.ProgramName = Program.ProgramName
                WHERE
                    PartName LIKE ?
            """, [x.part.replace("-", "%", 1)])

            for row in cursor.fetchall():
                nested["{}:\t\t\t({})".format(row.prog, row.mach)].append(row.part)

    for k, v in nested.items():
        print(k)
        for x in v:
            print("  - {}".format(x))


if __name__ == "__main__":
    main()
