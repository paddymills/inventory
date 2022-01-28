
import pyodbc

from os import getenv
from datetime import datetime
from string import Template

SNDB_PRD = "HIIWINBL18"
SNDB_DEV = "HIIWINBL5"

CONN_STR_USER_AUTH = Template("DRIVER={$driver};SERVER=$server;UID=$user;PWD=$pwd;DATABASE=$db;")
CONN_STR_WIN_AUTH = Template("DRIVER={$driver};SERVER=$server;Trusted_connection=yes;")

if len(pyodbc.drivers()) > 0:
    DEFAULT_DRIVER = pyodbc.drivers()[0]
else:
    raise NotImplementedError("No SQL drivers available")

class DbConnection:
    """
        pyodbc connection wrapper for databases
    """

    def __init__(self, use_win_auth=False, **kwargs):
        if use_win_auth:
            self.CS_TEMP = CONN_STR_WIN_AUTH
        else:
            self.CS_TEMP = CONN_STR_USER_AUTH

        self.driver = DEFAULT_DRIVER
        self.__dict__.update(kwargs)

        self._cnxn = None
        self._cur = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cnxn.close()

    def _make_cnxn(self):
        self._cnxn = pyodbc.connect(self.CS_TEMP.substitute(**self.__dict__))

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

class SndbConnection(DbConnection):
    """
        db connection wrapper for SigmaNest databases
    """

    def __init__(self, dev=False, **kwargs):
        init_kwargs = dict(
            server=SNDB_PRD,
            db="SNDBase91",
            user=getenv('SNDB_USER'),
            pwd=getenv('SNDB_PWD'),
        )

        init_kwargs.update(kwargs)
        super().__init__(**init_kwargs)

        if dev:
            self.server = SNDB_DEV
            self.db = "SNDBaseDev"
