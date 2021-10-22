
from lib import parsers
from lib import sndb

from collections import defaultdict
from tabulate import tabulate
from tqdm import tqdm

def main():
    data = parsers.parse_sheet()
    parts = defaultdict(int)
    for d in data:
        parts[d.part] += d.qty
    
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()

        states = dict(burned=2, nested=3)
        qtys = [['part', 'sap', *states.keys()]]
        for part, qty in tqdm(parts.items()):
            cursor.execute("""
                SELECT
                    'nested' AS state,
                    PartName AS part,
                    ProgramName AS prog,
                    QtyInProcess AS qty
                FROM PIP
                WHERE
                    PartName LIKE ?
                AND
                    WONumber NOT IN ('EXTRAS', 'REMAKES')

                UNION

                SELECT
                    'burned' AS state,
                    PartName AS part,
                    ProgramName AS prog,
                    QtyInProcess AS qty
                FROM PIPArchive
                WHERE
                    PartName LIKE ?
                AND
                    WONumber NOT IN ('EXTRAS', 'REMAKES')
                AND
                    TransType='SN102'
            """, [part.replace("-", "%", 1)] * 2)

            qty_burned, qty_nested = (0, 0)
            qrow = [part, qty, 0, 0]
            for row in cursor.fetchall():
                try:
                   qrow[states[row.state]] += int(row.qty)
                except KeyError:
                    print('Unknown state:', row)

            if qrow[1] < qrow[2]:
                qtys.append(qrow)

    print(tabulate(qtys, headers='firstrow'))


if __name__ == "__main__":
    main()
