
import xlwings

from lib import db
from tabulate import tabulate
from tqdm import tqdm

def try1():
    sndb = db.SndbConnection().cursor
    bom = db.BomConnection()

    sndb.execute("SELECT DISTINCT Data1, Data2 from Part where WONumber LIKE '%-%'")
    js = list(sndb.fetchall())

    for (job, shipment) in tqdm(js):
        results = [["Job", "Shipment", "Part", "Bom", "Sndb"]]
        for part in bom.get_bom(job, shipment):
            if part.desc.startswith("PL"):
                sndb.execute("""
                    SELECT
                        QtyOrdered
                    FROM Part
                    WHERE Data1=?
                    AND Data2=?
                    AND PartName=?

                    UNION

                    SELECT
                        QtyOrdered
                    FROM PartArchive
                    WHERE Data1=?
                    AND Data2=?
                    AND PartName=?
                    
                    UNION
                    
                    SELECT 0
                """, [job, shipment, "{}_{}".format(job, part.mark)] * 2)
                qty = int(sndb.fetchone()[0])
                if part.qty > qty:
                    results.append([job, shipment, part.mark, part.qty, qty])

    print(tabulate(results, headers="firstrow"))


def try2():
    key = lambda l: (l[0].split("-")[0], int(l[3]))

    sheet = xlwings.books.active.sheets.active

    data = sheet.range("B2:E2").expand('down').value
    vals = sorted(set([key(x) for x in data]))

    with db.SndbConnection() as sn:
        cursor = sn.cursor
        cursor.execute("""
            SELECT DISTINCT Data1, Data2 FROM Part WHERE Data1 LIKE '12%' AND QtyCompleted > 0
            UNION
            SELECT DISTINCT Data1, Data2 FROM PartArchive WHERE Data1 LIKE '12%'
        """)

        for j, s in cursor.fetchall():
            try:
                key = (j, int(s))
                vals.remove(key)
            except: pass

    for x in vals:
        print(x)

if __name__ == "__main__":
    # try1()
    try2()
