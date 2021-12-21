
from argparse import ArgumentParser
from os import path
from lib import printer

base = r"\\hssieng\SNData\SimTrans\Processed"


def get_sheet_data(sheetname):
    filename = path.join(base, "Stock_{}.txt".format(sheetname.upper()))

    if path.exists(filename):
        with open(filename, 'r') as stockfile:
            data = stockfile.read().split(',')

            res = dict()
            res["Sheet"] = data[4]
            res["Qty"] = data[5]
            res["Matl Grade"] = data[6]
            res["Thickness"] = data[7]
            res["Length"] = data[14]
            res["Width"] = data[15]
            res["UoM"] = data[16]
            res["Cost Per lb"] = data[18]
            res["Heat Number"] = data[19]
            res["PO Number"] = data[23]
            res["Location"] = data[24]
            res["WBS Element"] = data[25]
            res["SAP MM"] = data[26]
            res["BOL"] = data[28]
            res["Matl Doc"] = data[29]

            return res


if __name__ == '__main__':
    ap = ArgumentParser()
    ap.add_argument("sheet", nargs="+", help="Sheet to look up")

    args = ap.parse_args()
    if args.sheet:
        data = [['Sheet', 'SAP MM', 'Heat Number', 'PO Number']]
        for a in args.sheet:
            data.append([None] * len(data[0]))
            for k, v in get_sheet_data(a).items():
                if k in data[0]:
                    data[-1][data[0].index(k)] = v

        printer.print_to_source(data)
