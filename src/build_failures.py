
from argparse import ArgumentParser
from datetime import datetime
from os import path, listdir
from re import compile as regex
from tqdm import tqdm

base = r"\\hssieng\SNData\SimTrans\SAP Data Files"
timestamp = "Production_{:%Y%m%d%H%M%S}.ready".format(datetime.now())
output_filename = path.join(base, "other", timestamp)

NUM_FILES = 200
WBS_RE = regex(r"(?:[SD]-)?(?:\d{7}?-)?(\d{5})")

class FailuresFinder:

    def __init__(self):
        ap = ArgumentParser()
        ap.add_argument("--txt", action="store_true", help="read from text file")
        ap.add_argument("--all", action="store_true", help="process all files")

        self.args = ap.parse_args()

    @property
    def files(self):
        _files = sorted(listdir(path.join(base, "deleted files")), reverse=True)
        if self.args.all:
            return _files

        return _files[:NUM_FILES]

    def run(self):
        if self.args.txt:
            return self.from_txt()

        return self.from_input()

    def from_input(self):
        print("Enter args in order -> job mark program wbs_id")
        print("===================================================")

        args = [None] * 4
        while 1:
            val = input("> ")
            if not val:
                break

            temp_args = val.upper().split(" ")
            args = args[:len(args) - len(temp_args)] + temp_args
            args[-1] = WBS_RE.match(args[-1]).group(1)

            self.find_data(*args)

        print("file placed at: {}".format(output_filename))
        input("press any key to end")

    def from_txt(self):
        args = [None] * 4

        with open("input.txt", "r") as input_txt:
            for line in input_txt.readlines():
                temp_args = line.strip().upper().split(" ")
                args = args[:len(args) - len(temp_args)] + temp_args

                self.find_data(*args)

    def find_data(self, job, mark, prog, wbs_id):
        part = "{}-{}".format(job, mark)
        wbs = "D-{}-{}".format(job[:-1], wbs_id)

        for f in tqdm(self.files, desc="Finding {}".format((part, wbs, prog)), leave=False):
            with open(path.join(base, "deleted files", f), 'r') as prod_file:
                for row in prod_file.readlines():
                    vals = row.split('\t')
                    if vals[0].upper() == part and vals[2] == wbs and vals[12] == prog:
                        with open(output_filename, 'a') as res_file:
                            res_file.write(row)

                        return
        else:
            print("Row not found", (part, wbs, prog))


if __name__ == "__main__":
    finder = FailuresFinder()
    finder.run()
