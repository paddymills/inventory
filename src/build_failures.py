
from argparse import ArgumentParser
from datetime import datetime
from os import path, listdir
from pyautogui import hotkey, press
from re import compile as regex
from tqdm import tqdm

from lib.rpa import ObjectLocation
from lib import paths

sap_archive = path.join(paths.SAP_SIGMANEST_PRD, "Archive")
timestamp = "Production_{:%Y%m%d%H%M%S}.ready".format(datetime.now())
output_filename = path.join(paths.SAP_DATA_FILES, "other", timestamp)

# regular expressions
INBOX_TEXT_1 = regex(r"Planned order not found for (\d{7}[a-zA-Z])-([\w-]+), [SD]-\d{7}-(\d{5}), ([\d,]+).000, Sigmanest Program:([\d-]+)")
PROD_FILES = regex(r"Production_\d{14}.ready")
ARCHIVE_FILES = regex(r"Production_\d{14}.outbound.archive")

JOB_RE = regex(r"\d{7}[a-zA-Z]")
JOB_MARK_RE = regex(r"\d{7}[a-zA-Z]-[a-zA-Z0-9-]+")
WBS_RE = regex(r"(?:[SD]-)?(?:\d{7}?-)?(\d{5})")
PROG_RE = regex(r"\d{5}")
SPLIT_RE = regex(r"[\t, ]")

ObjectLocation.DEFAULT_DELAY = 1.0

OUTFILE_DEFAULT = "tmp/Production_{}.ready".format(datetime.now().strftime("%Y%m%d%H%M%S"))

class FoundState:

    FULL_MATCH      = 1
    WITHOUT_WBS     = 2
    WITHOUT_PROG    = 3
    NONE            = 4

class FailuresFinder:

    def __init__(self):
        self.ap = ArgumentParser()
        self.ap.add_argument("-a", "--all", action="store_true", help="process all files")
        self.ap.add_argument("-s", "--sap", action="store_true", help="pull data from SAP archive")
        self.ap.add_argument("-t", "--txt", action="store_true", help="read from text file")
        self.ap.add_argument("-n", "--name", action="store", help="save output to tmp dir as [name]")
        self.ap.add_argument("--nowbs", action="store_true", help="no wbs option for read from text file")
        self.ap.add_argument("-l", "--loop", action="store_true", help="loop to get input (adjust offsets first please)")
        self.ap.add_argument("-m", "--max", action="store", type=int, default=200, help="max files to process (default: 200)")
        self.ap.add_argument("--skip", action="store", type=int, default=0, help="files to skip before processing (default: 0)")
        self._args = None

        self.job   = None
        self.mark  = None
        self._wbs   = None
        self.prog  = None

        self.in2 = None

        self.found = FoundState.NONE

    @property
    def args(self):
        if self._args is None:
            self._args = self.ap.parse_args()

        return self._args


    def with_args(self, *args):
        self._args = self.ap.parse_args(args)

        return self

    @property
    def files(self):
        def get_files(folder, file_re):
            return sorted([path.join(folder, f) for f in listdir(folder) if file_re.match(f)], reverse=True)

        if self.args.sap:
            _files = get_files(sap_archive, ARCHIVE_FILES)
        else:
            _files = get_files(path.join(paths.SAP_DATA_FILES, "original"), PROD_FILES)

        if self.args.all:
            return _files

        # IDEA: once a part is found, we have likely found a neigborhood for parts from the same job
        return _files[self.args.skip:self.args.skip+self.args.max]

    @property
    def wbs(self):
        if self._wbs:
            return "D-{}-{}".format(self.job[:-1], self._wbs)

        return None

    @property
    def part(self):
        return "{}-{}".format(self.job, self.mark)

    def swap_wbs_and_program(self):
        self._wbs, self.prog = self.prog, self._wbs

    def run(self):
        if self.args.loop:
            return self.loop_input()

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

    def loop_input(self):
        display_workflow_log = ObjectLocation("Display Workflow Log")
        expand_data = ObjectLocation("Expand SigmanestInputData arrow")
        back_arrow = ObjectLocation("Back/Up Arrow (back to Inbox)")

        print("Enter args in order -> job mark wbs program")
        print("===================================================")

        while 1:
            display_workflow_log.click()
            expand_data.click()
            hotkey('alt', 'tab')

            val = input("> ")
            if not val:
                break

            try:
                iters = 0
                while 1:
                    if iters == 1:
                        self.swap_wbs_and_program()
                    elif iters > 1:
                        self.swap_wbs_and_program()
                        val = input("\t> ")
                        if not val:
                            break

                    if iters != 1:
                        self.parse_args(val)
                    
                    if self.find_data():
                        break

                    iters += 1


            except RuntimeError as e:
                print(e)

            back_arrow.click()
            press('down')

        print("file placed at: {}".format(output_filename))

    def from_txt(self):
        with open("tmp/input.txt", "r") as input_txt:
            for line in input_txt.readlines():
                try:
                    self.parse_args(line)
                except RuntimeError:
                    print("Error parsing line:", line)
                    return

                self.find_data()

    def parse_args(self, argstr):
        argstr = argstr.strip()

        if INBOX_TEXT_1.match(argstr):
            self.job, self.mark, self._wbs, self.in2, self.prog = INBOX_TEXT_1.match(argstr).groups()
            self.in2 = self.in2.replace(",", "")
            return

        self.in2 = None
        for arg in SPLIT_RE.split(argstr.upper()):
            if JOB_MARK_RE.match(arg):
                self.job, self.mark = arg.split("-", 1)

            elif JOB_RE.match(arg):
                self.job = arg

            elif arg.startswith('5') and PROG_RE.match(arg):
                self.prog = arg

            elif WBS_RE.match(arg):
                self._wbs = WBS_RE.match(arg).group(1)
                found_wbs = True

            else:   # part
                self.mark = arg

        if None in (self.job, self.prog, self.mark) and (not self.args.nowbs and self._wbs is None):
            raise RuntimeError("Not all arguments were provided")

    def find_data(self):
        if self.args.name:
            output_filename = "tmp/{}.ready".format(self.args.name)
        else:
            output_filename = OUTFILE_DEFAULT

        self.found = FoundState.NONE
        for f in tqdm(self.files, desc="Finding {}".format((self.part, self.wbs, self.prog)), leave=False):
            with open(f, 'r') as prod_file:
                for row in prod_file.readlines():
                    if self.row_is_match(row):
                        with open(output_filename, 'a') as res_file:
                            res_file.write(row)

                        if self.found == FoundState.FULL_MATCH:
                            return True

                # break if found and not searching without program
                if self.found == FoundState.WITHOUT_WBS:
                            return True

        if self.found == FoundState.NONE:
            print("Row not found", (self.part, self.wbs, self.prog))
            return False
        
        return True

    def row_is_match(self, row):
        # checks if row is a match
        # returns match type, which determines loop exit
        PART_INDEX = 0
        WBS_INDEX = 2
        PROGRAM_INDEX = 12
        IN2_INDEX = 4

        vals = row.split('\t')
        if vals[PART_INDEX].upper() != self.part:
            return False
        if self.wbs and vals[WBS_INDEX] != self.wbs:
            return False
        if self.prog and vals[PROGRAM_INDEX] != self.prog:
            return False

        # if in2 is supplied (parsed from inbox text), skip when mismatched in2
        if self.in2 and vals[IN2_INDEX] != self.in2:
            return False

        # if we made it this far, then we can assume:
        #   - part is a match
        #   - wbs is a match or searching without a wbs
        #   - program is a match or searching without a program
        if self.prog is None:
            self.found = FoundState.WITHOUT_PROG
        elif self.wbs is None:
            self.found = FoundState.WITHOUT_WBS
        else:
            self.found = FoundState.FULL_MATCH

        
        return True


if __name__ == "__main__":
    finder = FailuresFinder()
    finder.run()
