
import pyodbc

from os import getenv
from datetime import datetime
from string import Template

SNDB_PRD = "HIIWINBL18"
SNDB_DEV = "HIIWINBL5"

CONN_STR_TEMPLATE = Template(
    "DRIVER={$driver};SERVER=$server;UID=$user;PWD=$pwd;DATABASE=$db;")


class SndbConnection:
    """
        pyodbc connection wrapper for sigmanest databases
    """

    def __init__(self, dev=False, **kwargs):
        self.__dict__.update(kwargs)

        self._cnxn = None
        self._cur = None
        self.cs_kwargs = dict(
            driver="SQL Server",
            server=SNDB_PRD,
            db="SNDBase91",
            user=getenv('SNDB_USER'),
            pwd=getenv('SNDB_PWD'),
        )
        self.cs_kwargs.update(kwargs)
        if dev:
            self.cs_kwargs['server'] = SNDB_DEV
            self.cs_kwargs['db'] = "SNDBaseDev"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cnxn.close()

    def _make_cnxn(self):
        self._cnxn = pyodbc.connect(CONN_STR_TEMPLATE.substitute(**self.cs_kwargs))

    @property
    def connection(self):
        if not self._cnxn:
            self._make_cnxn()

        return self._cnxn

    @property
    def cursor(self):
        if not self._cur:
            self._cur = self.connection.cursor()

        return self._cur

    def commit(self):
        self.connection.commit()

    def execute_sql_file(self, file_path, *args):
        with open(file_path, 'r') as sql_file:
            sql = sql_file.read()

            self.cursor.execute(sql, *args)

        return self

    def collect_table_data(self):
        min_date = datetime(1900, 1, 1)

        data = [None]
        for row in self.cursor.fetchall():
            data.append(row)

            # replace active dates (1900-01-01 00:00:00 -> --)
            if min_date in data[-1]:
                data[-1] = ["--" if x == min_date else x for x in data[-1]]
        else:
            try:
                data[0] = [t[0] for t in row.cursor_description]
            except UnboundLocalError:
                caller = self.__dict__.get("func", "-- no origin --")
                return [["values"], ["nothing returned ({})".format(caller)]]

        return data
