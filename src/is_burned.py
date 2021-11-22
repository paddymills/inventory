
import csv
import sys

from lib import parsers
from lib import sndb
from lib import printer

from argparse import ArgumentParser
from collections import defaultdict
from tabulate import tabulate
from tqdm import tqdm

def main(as_csv=False):
    data = parsers.parse_sheet()
    parts = defaultdict(lambda: [0, 0])
    for d in data:
        if d.type == "PP01":
            parts[d.part][0] += d.qty
        elif d.type == "PR":
            parts[d.part][1] += d.qty
    
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()

        states = dict(
            required=1,
            nested=2,
            burned=3,
            nested_rem=4,
            burned_rem=5,
        )
        qtys = [['part', *states.keys(), 'open', 'cnf']]
        for part, _qtys in tqdm(parts.items()):
            qty_cnf, qty_planned = _qtys

            cursor.execute("""
                SELECT *
                FROM (
                    SELECT
                        'required' AS state,
                        PartName AS part,
                        QtyOrdered AS qty,
                        WONumber AS workorder
                    FROM Part
                    WHERE
                        WONumber NOT IN ('EXTRAS', 'REMAKES')

                    UNION

                    SELECT
                        'required' AS state,
                        PartName AS part,
                        QtyOrdered AS qty,
                        WONumber AS workorder
                    FROM PartArchive
                    WHERE
                        WONumber NOT IN ('EXTRAS', 'REMAKES')

                    UNION

                    SELECT
                        'nested' AS state,
                        PartName AS part,
                        QtyInProcess AS qty,
                        WONumber AS workorder
                    FROM PIP

                    UNION

                    SELECT
                        'burned' AS state,
                        PartName AS part,
                        QtyInProcess AS qty,
                        WONumber AS workorder
                    FROM PIPArchive
                    WHERE
                        TransType='SN102'
                ) AS PartAndPIP
                WHERE
                    part LIKE ?  
            """, "%" + part[4:].replace("-", "%", 1))

            qrow = [part, *[0] * len(states), int(qty_planned), int(qty_cnf)]
            for row in cursor.fetchall():
                if row.workorder in ('EXTRAS', 'REMAKES'):
                    addl = "_rem"
                else:
                    addl = ""

                try:
                   qrow[states[row.state + addl]] += int(row.qty)
                except KeyError:
                    print('Unknown state:', row)

            # if qrow[1] >= qrow[2]:
            #     continue
            qtys.append(qrow)

    printer.print_to_source(qtys, as_csv)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Return as csv output")
    args = parser.parse_args()

    main(args.csv)
