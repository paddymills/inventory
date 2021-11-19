
import csv

from datetime import datetime
from sys import stdout
from tabulate import tabulate


def print_to_source(results, as_csv=False):
    if as_csv:
        csv.writer(stdout).writerows(results)
    else:
        print(tabulate(results, headers="firstrow"))
