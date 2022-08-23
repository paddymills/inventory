
from datetime import date
from os import path
import xlwings

from lib import db

ONEDRIVE_INV = r"C:\Users\pmiller1\OneDrive - high.net\inventory_recon"


def main():
	sn = get_sn_data()
	sap = get_sap_data()

	dump_data(sn, sap)


def dump_data(sn, sap):
	wb = xlwings.Book(path.join(ONEDRIVE_INV, "template_dev.xlsx"))

	sheet = wb.sheets("Sigmanest")
	sheet.range("A1").value = sn
	sheet.autofit()

	if sap:
		sheet = wb.sheets("SAP")
		sheet.range("A1").value = sap

	save_name = date.today().strftime("%Y-%m-%d")
	count = 1
	while path.exists(path.join(ONEDRIVE_INV, save_name)):
		save_name = "{}_{}".format(save_name[:10], count)
		count += 1
	print("save:", save_name)


def get_sn_data():
	with db.SndbConnection() as conn:
		conn.cursor.execute("""
			SELECT
				Plant, Location, MaterialMaster, Wbs,
				FORMAT(SUM(Area), 'F3', 'en-US') AS Area,
				UoM
			FROM (
				SELECT
					IIF(LEFT(SheetName, 1)='W', 'HS02', 'HS01') AS Plant,
					Location,
					PrimeCode AS MaterialMaster,
					Mill AS Wbs,
					Qty * Area AS Area,
					Remark as UoM
				FROM
					Stock
				WHERE
					SheetName NOT LIKE 'SLAB%'
			) AS Stock
			GROUP BY
				Plant, Location, MaterialMaster, Wbs, UoM
			ORDER BY
				Plant, Location, MaterialMaster, Wbs
		""")

		return conn.collect_table_data()


def get_sap_data():
	try:
		wb = xlwings.books["export.XLSX"]
		data =  wb.sheets[0].range("A1").expand().value

		while not data[-1][0]:
			del data[-1]

		return data

	except KeyError:
		print("SAP export file not open. You will have to manually copy the data.")


if __name__ == "__main__":
	main()
