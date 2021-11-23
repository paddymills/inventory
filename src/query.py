
import csv
import sys
import re

from lib import sndb
from lib import printer

from argparse import ArgumentParser
from datetime import datetime
from tabulate import tabulate


def main(val=None, force_matl=False):
    if not val:
        val = input("\n\tIdentifier: ")
        if not val:
            return

    val = val.upper()

    REGEX_MAP = [
        (r"\d{7}[A-Z]\d{0,2}-\d{2,5}[A-Z]?", query_matl),
        (r"(?:9-)?(?:HPS)?50W?(?:/50W)?[TF]?\d?-\d{4}[A-Z]?", query_matl),
        (r"[A-Z]{1,2}\d{5,}(?:-\d)?", query_sheet),
    ]

    if force_matl:
        return query_matl(val)

    for pattern, func in REGEX_MAP:
        if re.match(pattern, val):
            return func(val)
    else:
        return query_part(val)


def query_matl(matl):
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT * FROM
            (
                SELECT
                    Program.ArcDateTime AS Timestamp,
                    Stock.Location AS Location,
                    Stock.PrimeCode AS Matl,
                    Stock.SheetName AS Sheet,
                    Program.ProgramName AS Program,
                    Stock.HeatNumber AS Heat,
                    Stock.BinNumber AS PO
                FROM StockArchive AS Stock
                    INNER JOIN ProgArchive AS Program
                        ON Stock.SheetName=Program.SheetName
                WHERE Stock.PrimeCode LIKE ? AND Program.TransType='SN102'
                UNION
                SELECT
                    0 AS Timestamp,
                    Stock.Location AS Location,
                    Stock.PrimeCode AS Matl,
                    Stock.SheetName AS Sheet,
                    Program.ProgramName AS Program,
                    Stock.HeatNumber AS Heat,
                    Stock.BinNumber AS PO
                FROM Stock
                    LEFT JOIN Program
                        ON Stock.SheetName=Program.SheetName
                WHERE Stock.PrimeCode LIKE ?
            ) AS Stock
            ORDER BY Timestamp DESC
        """, [matl, matl])

        return sndb.collect_table_data(cursor, "Material")


def query_sheet(sheet):
    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT * FROM
            (
                SELECT
                    Program.ArcDateTime AS Timestamp,
                    Stock.Location AS Location,
                    Stock.PrimeCode AS Matl,
                    Stock.SheetName AS Sheet,
                    Program.ProgramName AS Program,
                    Stock.HeatNumber AS Heat,
                    Stock.BinNumber AS PO
                FROM StockArchive AS Stock
                    INNER JOIN ProgArchive AS Program
                        ON Stock.SheetName=Program.SheetName
                WHERE Stock.SheetName LIKE ? AND Program.TransType='SN102'
                UNION
                SELECT
                    0 AS Timestamp,
                    Stock.Location AS Location,
                    Stock.PrimeCode AS Matl,
                    Stock.SheetName AS Sheet,
                    Program.ProgramName AS Program,
                    Stock.HeatNumber AS Heat,
                    Stock.BinNumber AS PO
                FROM Stock
                    LEFT JOIN Program
                        ON Stock.SheetName=Program.SheetName
                WHERE Stock.SheetName LIKE ?
            ) AS Stock
            ORDER BY Timestamp DESC
        """, [sheet, sheet])

        return sndb.collect_table_data(cursor, "Sheet")

def query_part(part):
    part = "%" + part.replace('-', '%') + "%"

    with sndb.get_sndb_conn() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT
                PIP.ArcDateTime AS Timestamp,
                PIP.PartName AS Part,
                PIP.QtyInProcess AS Qty,
                PIP.ProgramName AS Program,
                Stock.HeatNumber AS Heat,
                Stock.BinNumber AS PO,
                Stock.PrimeCode AS Matl
            FROM PIPArchive AS PIP
                INNER JOIN StockArchive AS Stock
                    ON PIP.ProgramName=Stock.ProgramName
            WHERE PIP.PartName LIKE ? AND PIP.TransType='SN102'
            UNION
            SELECT
                0 AS Timestamp,
                PIP.PartName AS Part,
                PIP.QtyInProcess AS Qty,
                PIP.ProgramName AS Program,
                Stock.HeatNumber AS Heat,
                Stock.BinNumber AS PO,
                Stock.PrimeCode AS Matl
            FROM PIP
                INNER JOIN Program
                    ON PIP.ProgramName=Program.ProgramName
                INNER JOIN Stock
                    ON Program.SheetName=Stock.SheetName
            WHERE PIP.PartName LIKE ?
            ORDER BY PIP.ArcDateTime
        """, [part, part])

        return sndb.collect_table_data(cursor, "Part")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Return as csv output")
    parser.add_argument("--matl", action="store_true", help="Force query as material")
    parser.add_argument("part", nargs="?", default=None)
    args = parser.parse_args()

    res = main(args.part, args.matl)
    printer.print_to_source(res, args.csv)
