import pyodbc
import os
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
