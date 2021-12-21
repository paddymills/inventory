import pyodbc
import os
from datetime import datetime
from string import Template

SNDB_PRD = "HIIWINBL18"
SNDB_DEV = "HIIWINBL5"

CONN_STR_TEMPLATE = Template(
    "DRIVER={$driver};SERVER=$server;UID=$user;PWD=$pwd;DATABASE=$db;")

cs_kwargs = dict(
    driver="SQL Server",
    server=SNDB_PRD,
    db="SNDBase91",
    user=os.getenv('SNDB_USER'),
    pwd=os.getenv('SNDB_PWD'),
)


def get_sndb_conn(dev=False, **kwargs):
    cs_kwargs.update(kwargs)

    if dev:
        cs_kwargs['server'] = SNDB_DEV
        cs_kwargs['db'] = "SNDBaseDev"

    connection_string = CONN_STR_TEMPLATE.substitute(**cs_kwargs)

    return pyodbc.connect(connection_string)


def collect_table_data(cursor, func="origin not given"):
    min_date = datetime(1900, 1, 1)

    data = [None]
    for row in cursor.fetchall():
        data.append(row)

        # replace active dates (1900-01-01 00:00:00 -> --)
        if min_date in data[-1]:
            data[-1] = ["--" if x == min_date else x for x in data[-1]]
    else:
        try:
            data[0] = [t[0] for t in row.cursor_description]
        except UnboundLocalError:
            return [["values"], ["nothing returned ({})".format(func)]]

    return data
