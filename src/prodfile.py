
import csv
from tabulate import tabulate

data = dict()
with open("Production_20221118134511.ready") as c:
    reader = csv.reader(c, delimiter="\t")
    for row in reader:
        pair = "{} ({})".format(row[6], row[10])
        if pair not in data:
            data[pair] = 0.0
        data[pair] += float(row[8])

print(tabulate(sorted([[k, v] for k,v in data.items()])))
