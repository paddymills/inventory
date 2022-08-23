
import csv
from collections import defaultdict

class PlannedOrder:

    def __init__(self, row):
        self.wbs = row["WBS Element"]
        self.plant = row["Plant"]
        self.qty = int(row["Order quantity (GMEIN)"])

    def commit(self, qty=None):
        if qty is None:
            qty = self.qty

        assert qty <= self.qty, "Cannot commit more than order quantity"
        self.qty -= qty

def main():
    orders = get_orders()

    prod, issue = list(), list()
    with open(r"\\hssieng\sndata\simtrans\sap data files\other\Production.ready", "r") as ready:
        for line in ready.readlines():
            row = line.split("\t")

            mark = row[0].upper()
            qty = int(row[4])
            in2_per_ea = float(row[8]) / qty
            for order in orders[mark]:
                commit_qty = min(qty, order.qty)
                order.commit(commit_qty)

                row[2] = order.wbs
                row[4] = str(commit_qty)
                row[8] = str(round(in2_per_ea * commit_qty, 3))
                row[11] = order.plant
                prod.append("\t".join(row))

                qty -= commit_qty

                if qty == 0:
                    break

            else:
                row[4] = str(qty)
                issue.append("\t".join(row))

            # clear out zero qty orders
            orders[mark] = [x for x in orders[mark] if x.qty > 0]

    with open (r"tmp\Production.ready", "w") as f:
        f.writelines(prod)

    with open (r"tmp\Issue.ready", "w") as f:
        f.writelines(issue)


def get_orders():
    orders = defaultdict(list)
    # get number of each part confirmed from export.csv
    with open("tmp/export.csv", "r") as export:
        for row in csv.DictReader(export):
            if row["Order Type"] != "PR":
                continue
            
            orders[row["Material Number"].upper()].append( PlannedOrder(row) )

    return orders

if __name__ == "__main__":
    main()
