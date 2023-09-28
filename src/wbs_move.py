
from collections import defaultdict
import re
from tqdm import tqdm
import xlwings

desc_pattern = re.compile(r"(?:Shipment|Stage) ([A-Z])([0-9]{1,2})")
wbs_pattern = re.compile(r"D-\d{7}-(\d{5})")
wbs_item_pattern = re.compile(r"\d{5}$")

job_structs = defaultdict(list)
def main():
    wb = xlwings.books.active
    for wbs, desc in tqdm(wb.sheets['NewWBS'].range("A2").expand().value, desc="getting new WBS map"):
        if desc_pattern.match(desc):
            js = "{}{}".format(wbs[2:9], desc_pattern.match(desc).group(1))
            wbs_item = int(wbs_pattern.match(wbs).group(1))
            job_structs[js].append(wbs_item)

    for key, value in tqdm(job_structs.items(),desc="sorting..."):
        job_structs[key] = sorted(value)

    sheet = wb.sheets['Move']
    end = sheet.range("A1").expand('down').end('down').row
    for row in tqdm(range(2, end+1), desc="Setting new WBS"):
        old_wbs_value = sheet.range(row, 8).value
        old_wbs_item = int(wbs_pattern.match(old_wbs_value).group(1))
        js = sheet.range(row, 1).value[:8]
        new_wbs_item = None
        for wbs_item in job_structs[js]:
            if wbs_item < old_wbs_item:
                new_wbs_item = wbs_item
            if wbs_item == old_wbs_item:
                new_wbs_item = None
                sheet.range(row, 9).value = "--"
                break

        if new_wbs_item:
            sheet.range(row, 9).value = wbs_item_pattern.sub(str(new_wbs_item), old_wbs_value)

if __name__ == "__main__":
    main()
