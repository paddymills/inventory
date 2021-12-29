
import re

from lib.sndb import SndbConnection
from lib import printer
from argparse import ArgumentParser
from os import path

REGEX_MAP = [
    # Regex Pattern                                         name
    (r"\d{7}[A-Z]\d{0,2}-\d{2,5}[A-Z]?",                    "matl"),
    (r"(?:9-)?(?:HPS)?50W?(?:/50W)?[TF]?\d?-\d{4}[A-Z]?",   "matl"),
    (r"[A-Z]{1,2}\d{5,}(?:-\d)?",                           "sheet"),
]

REPLACEMENTS = [
    ("*", "%"),
    ("+", "_"),
]


def main():
    parser = ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Return as csv output")
    parser.add_argument("--matl", action="store_true", help="Force query as material")
    parser.add_argument("--sql", help="Value to query")
    parser.add_argument("part", nargs="?", default=None)
    args = parser.parse_args()

    if args.sql:
        return query_sql(args.sql)

    val = args.part

    # ask for value if not given
    if not val:
        val = input("\n\tIdentifier: ")
        if not val:
            return

    # uppercase and replace wildcards
    val = val.upper()
    for _find, _replace in REPLACEMENTS:
        val = val.replace(_find, _replace)

    query = get_query_type(val, args.matl)

    with SndbConnection(func=query) as db:
        if query == "part":
            val = "%" + val.replace('-', '%') + "%"

        sql_file = path.join(path.dirname(__file__), "sql", "query_{}.sql".format(query))
        db.execute_sql_file(sql_file, [val, val])
        sql_data = db.collect_table_data()

        printer.print_to_source(sql_data, args.csv)


def get_query_type(val, force_matl):
    if force_matl:
        return "matl"

    for pattern, name in REGEX_MAP:
        if re.match(pattern, val):
            return name

    return "part"


def query_sql():
    sql = input("\n\tSQL> ")
    if not sql:
        return

    with SndbConnection(func="SQL Statement") as db:
        db.execute(sql)

        return printer.print_to_source(db.collect_table_data())


if __name__ == "__main__":
    main()
