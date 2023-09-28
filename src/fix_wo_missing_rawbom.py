
import os
import xlwings

from lib.db import SndbConnection
from lib.paths import workorder_file

def add_bom():
    raw_bom_file = "1220173A-1,2_Raw_BOM_HD.xlsm"
    tagschedule_file = "1220173A-1.xls"

    # set directory to script root
    os.chdir(os.path.dirname(__file__))

    wb = xlwings.Book(raw_bom_file)
    sheet = wb.sheets.active

    parts = dict()
    for line in sheet.range("A2:J2").expand("down").value:
        if line[-1] == "SigmaNest ~ Do Not Load":
            continue
        parts[line[0].split("-", 1)[1]] = line[4]

    print("updating database")
    with SndbConnection() as db:
        cur = db.cursor
        for part, mm in parts.items():
            cur.execute("""
                UPDATE Part
                SET Data10=?
                WHERE Data9=?
            """, mm, part)
        cur.commit()


def fix_general():
    print("updating database")
    with SndbConnection() as db:
        cur = db.cursor
        cur.execute("""
            update Part
            set Data10='', Data11=''
            -- where not (WONumber like '%-%-WEBS' or WONumber like '%-%-FLGS')
            where WONumber in ('1220208C-3-WEBS', '1220208C-3-FLGS')
                and substring(WONumber,1,8) != '1220173A'
                and substring(Data10,1,8) = '1220173A'
        """)
        cur.commit()
    print("done")


def fix_db(job, shipment, file_extra=None):
    print("Fixing: {}-{}".format(job, shipment))

    print("\t- Collecting data")
    parts = list()
    wb = xlwings.Book(workorder_file(job, shipment, file_extra))
    sheet = wb.sheets.active
    end = sheet.range("A3").expand('down').end('down').row
    parts = sheet.range("BI3:BK{}".format(end)).value
    wb.close()

    if type(parts[0]) is not list:
        parts = [parts]

    print("\t- updating database")
    with SndbConnection() as db:
        cur = db.cursor
        for part, mm, size in parts:
            if mm in (None, ''):
                continue

            cur.execute("""
                UPDATE Part
                SET Data10=?, Data11=?
                WHERE Data9=?
                AND Data1=? AND Data2=?
            """, mm, size, part, job, str(int(shipment)))
        cur.commit()

    print("\t- done")


if __name__ == "__main__":
    # add_bom()
    # fix_general()

    js = [
        ("1190181A", 1),
        ("1220208C", 3),
    ]
    for job_ship in js:
        fix_db(*job_ship)
