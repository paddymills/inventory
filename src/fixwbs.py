
import pyodbc
import pandas


CS = "DRIVER={SQL Server};SERVER=HIIWINBL18;DATABASE=SNDBase91;UID=SNUser;PWD=BestNest1445"

EXPORT = r"C:\Users\PMiller1\Documents\SAP\SAP GUI\export.XLSX"
# EXPORT = "/c/Users/PMiller1/Documents/SAP/SAP GUI/export.XLSX"

XL_HEADER = {
    "Material Number": "part",
    "Order quantity (GMEIN)": "qty",
    "WBS Element": "wbs",
    "Occurrence": "shipment",
    "Material description": "desc",
}

DB_HEADER = {
    "PartName": "part",
    "WBS": "wbs",
    "Shipment": "shipment",
}


def sqlFormat(df, columns):
    return df.filter(items=columns).values.tolist()


with pyodbc.connect(CS) as db:
    importedJobs = pandas.read_sql_query(
        """
            SELECT DISTINCT LEFT(PartName, 8) AS job, Shipment AS shipment
            FROM SAPPartWBS
        """, con=db)
    importedParts = pandas.read_sql_query(
        """
            SELECT PartName AS part, WBS AS wbs
            FROM SAPPartWBS
        """, con=db)

    # read only HEADER columns
    xl = pandas.read_excel(EXPORT)[XL_HEADER.keys()]
    xl.rename(columns=XL_HEADER, inplace=True)        # rename HEADER columns

    # keep PL*, SHT*, Sheet* desc and D-* WBS items
    filter1 = xl[xl.desc.str.startswith(('PL', 'SHT', 'Sheet'))]
    filter2 = xl[xl.wbs.str.startswith("D-", na=False)]

    df = pandas.merge(filter1, filter2)         # create dataframe from filters
    df['job'] = df['part'].str.slice(stop=8)    # add job column

    # reduce to items from imported Job-Shipment pairs
    merged = df.merge(importedJobs, how='left', on=[
                      'job', 'shipment'], indicator=True)
    imported = merged[merged['_merge'] == 'both'].copy()

    # merge imported with database Part-WBS pairs
    # delete _merge from previous
    imported.drop('_merge', axis=1, inplace=True)
    merged = imported.merge(importedParts, how='left', on=[
                            'part', 'wbs'], indicator=True)

    # items in SAP & Database
    updates = merged[merged['_merge'] == 'both']
    additions = merged[merged['_merge'] == 'left_only']  # items in SAP only

    # update database
    cursor = db.cursor()
    # cursor.fast_executemany = True

    print("SNDBase91 :: SAPPartWBS [Updating Existing...]")
    cursor.execute("UPDATE SAPPartWBS SET QtyConf=QtyReq")
    cursor.executemany(
        """
            UPDATE SAPPartWBS SET QtyConf=QtyReq-?
            WHERE PartName=? AND WBS=?
        """,
        sqlFormat(updates, columns=["qty", "part", "wbs"])
    )

    if not additions.empty:
        print("SNDBase91 :: SAPPartWBS [Adding New...]")
        cursor.executemany(
            """
                INSERT INTO SAPPartWBS (PartName, WBS, QtyReq, Shipment)
                VALUES (?, ?, ?, ?)
            """,
            sqlFormat(additions, columns=["part", "wbs", "qty", "shipment"])
        )

    cursor.commit()
