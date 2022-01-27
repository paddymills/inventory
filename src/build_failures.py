
from argparse import ArgumentParser
from datetime import datetime
from os import path, listdir
from re import compile as regex
from tqdm import tqdm

base = r"\\hssieng\SNData\SimTrans\SAP Data Files"
timestamp = "Production_{:%Y%m%d%H%M%S}.ready".format(datetime.now())
output_filename = path.join(base, "other", timestamp)

NUM_FILES = 200
JOB_RE = regex(r"\d{7}[a-zA-Z]")
WBS_RE = regex(r"(?:[SD]-)?(?:\d{7}?-)?(\d{5})")
PROG_RE = regex(r"\d{5}")

class FailuresFinder:

    def __init__(self):
        ap = ArgumentParser()
        ap.add_argument("--txt", action="store_true", help="read from text file")
        ap.add_argument("--all", action="store_true", help="process all files")

        self.args = ap.parse_args()

        self.find_job   = None
        self.find_mark  = None
        self.find_wbs   = None
        self.find_prog  = None

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
        print("Enter args in order -> job mark wbs program")
        print("===================================================")

        while 1:
            val = input("> ")
            if not val:
                break

            try:
                self.parse_args(val)

            except RuntimeError as e:
                print(e)
                continue

            self.find_data()

        print("file placed at: {}".format(output_filename))
        input("press any key to end")

    def from_txt(self):
        with open("input.txt", "r") as input_txt:
            for line in input_txt.readlines():
                try:
                    self.parse_args(line)
                except RuntimeError as e:
                    print("Error parsing line:", line)
                    return

                self.find_data()

    def parse_args(self, argstr):
        found_wbs = False

        for arg in argstr.upper().split(" "):
                if JOB_RE.match(arg):
                    self.find_job = arg

                elif WBS_RE.match(arg) and not found_wbs:
                    self.find_wbs = WBS_RE.match(arg).group(1)
                    found_wbs = True

                elif PROG_RE.match(arg):
                    self.find_prog = arg

                else:   # part
                    self.find_mark = arg

        if None in self.find_vals:
            raise RuntimeError("Not all arguments were provided")

        return self.find_vals

    def find_data(self):
        part = "{}-{}".format(self.job, self.mark)
        wbs = "D-{}-{}".format(self.job[:-1], self.wbs)

        for f in tqdm(self.files, desc="Finding {}".format((part, wbs, self.find_prog)), leave=False):
            with open(path.join(base, "deleted files", f), 'r') as prod_file:
                for row in prod_file.readlines():
                    vals = row.split('\t')
                    if vals[0].upper() == part and vals[2] == wbs and vals[12] == self.find_prog:
                        with open(output_filename, 'a') as res_file:
                            res_file.write(row)

                        return
        else:
            print("Row not found", (part, wbs, self.find_prog))


if __name__ == "__main__":
    finder = FailuresFinder()
    finder.run()
