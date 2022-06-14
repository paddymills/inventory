
import csv
import os
import shutil

from argparse import ArgumentParser
from datetime import datetime
from collections import defaultdict
from tabulate import tabulate

from lib.db import SndbConnection
from lib.parsers import CnfFileParser


class Part:

    def __init__(self, name):
        self.name = name
        self.inbox = 0
        self.cnf = 0

        self._inbox = list()
        self.orders = list()

    @property
    def burned(self):
        with SndbConnection() as db:
            sn_part = self.name.replace("-", "_", 1)
            db.cursor.execute("""
                SELECT SUM(QtyInProcess)
                FROM PIPArchive
                WHERE TransType='SN102'
                GROUP BY PartName
                HAVING PartName LIKE ?
            """, sn_part)

            result = db.cursor.fetchone()
            if result:
                return result[0]

        return 0

    def tabular(self):
        return [self.name, self.burned, self.cnf, self.inbox, self.burned - self.cnf - self.inbox]

    def add_entry(self, entry):
        # print("Adding {}({}) to...".format(entry.program, entry.part_qty))
        # for row in self._inbox:
        #     print("\t", row)

        for n in self._inbox:
            if entry.program == n.program:
                try:
                    n += entry
                    return
                except AssertionError:  # mismatch in material master and/or wbs
                    pass

        self._inbox.append(entry)

    def fix(self):
        fixed = list()
        not_fixed = list()
        for p in sorted(self._inbox, key=lambda x: x.part_qty):
            for order in sorted(self.orders, key=lambda x: x.qty):
                p.part_wbs = order.wbs
                p.matl_plant = order.plant

                if p.part_qty <= order.qty:
                    fixed.append(p.output())
                    order.commit(p.part_qty)

                elif order.qty > 0:
                    fixed.append(p.output(qty=order.qty))
                    order.commit()
                
                if p.part_qty == 0:
                    break
            else:
                not_fixed.append(p.output())
        
        return fixed, not_fixed


class PlannedOrder:

    def __init__(self, wbs, plant, qty):
        self.wbs = wbs
        self.plant = plant
        self.qty = qty

    def commit(self, qty=None):
        if qty is None:
            qty = self.qty

        assert qty <= self.qty, "Cannot commit more than order quantity"
        self.qty -= qty


def main():
    ap = ArgumentParser()
    ap.add_argument("-i", "--init",      action="store_true", help="run pre-compare scripts")
    ap.add_argument("-m", "--move",      action="store_true", help="move fixed file to workflow box")
    ap.add_argument("-n", "--no-export", action="store_true", help="skip export")
    ap.add_argument("-p", "--parts",     action="store_true", help="export parts list")
    ap.add_argument("-t", "--tabulate",  action="store_true", help="print tabulated comparison")
    ap.add_argument("-s", "--sort",      action="store_true", help="sort inbox file")
    args = ap.parse_args()

    if args.init:
        args.parts = args.sort = True

    if args.sort:
        print("🧾 sorting inbox.ready ...")
        with open('tmp/inbox.ready', 'r+') as inboxfile:
            # read, split, and sort
            delim = [line.split("\t") for line in inboxfile.readlines()]
            sorted_data = sorted(delim, key=lambda r: (r[0], r[12]))

            # clear file
            inboxfile.truncate(0)
            inboxfile.seek(0)

            # sort, join and write
            inboxfile.writelines(["\t".join(line) for line in sorted_data])

    # get data
    parts = get_data()

    # show data
    if args.tabulate:
        print(tabulate(
            [v.tabular() for v in parts.values()],
            headers=["Part", "Burned", "Cnf", "Inbox", "Delta"]
        ))

    if args.parts:
        print("🧰 exporting parts.txt ...")
        with open('tmp/parts.txt', 'w') as partsfile:
            partsfile.write("\n".join(parts.keys()))

    elif args.move:
        print("🚀 exporting data ...")
        name = "Production_{:%Y%m%d%H%M%S}.ready".format(datetime.now())
        shutil.move("tmp/fixed.ready", os.path.join(r"\\hiifileserv1\sigmanestprd\Outbound", name))
        shutil.move("tmp/not_fixed.ready", "tmp/inbox.ready")

    elif not args.no_export:
        # try to fix things
        print("🛠️ fixing things ...")
        fixed = open("tmp/fixed.ready", "w")
        not_fixed = open("tmp/not_fixed.ready", "w")

        print("🛰️ generating data ...")
        for part in parts.values():
            f, n = part.fix()
            fixed.writelines(f)
            not_fixed.writelines(n)

        fixed.close()
        not_fixed.close()


def get_data():
    parts = dict()

    # get inbox errors
    with open("tmp/inbox.ready", 'r') as f:
        parser = CnfFileParser()
        for line in f.readlines():
            parsed = parser.parse_row(line.upper())

            if parsed.part_name not in parts:
                parts[parsed.part_name] = Part(parsed.part_name)

            part = parts[parsed.part_name]

            part.inbox += parsed.part_qty
            part.add_entry(parsed)

    # get number of each part confirmed from export.csv
    with open("tmp/export.csv", "r") as export:
        for row in csv.DictReader(export):
            part = row["Material Number"]
            wbs = row["WBS Element"]
            plant = row["Plant"]
            qty = int(row["Order quantity (GMEIN)"])

            order_type = row["Order Type"]

            if part not in parts:
                continue

            if order_type == "PP01":
                parts[part].cnf += qty
            elif order_type == "PR":
                parts[part].orders.append(PlannedOrder(wbs, plant, qty))
            else:
                print("unmatched order type:", order_type)

    return parts

if __name__ == "__main__":
    main()
