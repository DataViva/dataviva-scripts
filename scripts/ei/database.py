import os
import MySQLdb



''' Connect to DB '''
class DB(object):

    def __init__(self):
        host = os.environ.get("DATAVIVA_DB_HOST", "127.0.0.1")
        user = os.environ.get("DATAVIVA_DB_USER", "root")
        passwd = os.environ.get("DATAVIVA_DB_PW", None)
        dbname = os.environ.get("DATAVIVA_DB_NAME", "dataviva_dv2015")

        self.db = MySQLdb.connect(host=host, user=user, passwd=passwd, db=dbname)
        self.cursor = self.db.cursor()

    def make_dict(self, table, key, value, overide={}, type=None):
        self.cursor.execute("select %s, %s from %s " % (key, value, table))
        data = {str(r[0]):r[1] for r in self.cursor.fetchall()}

        overide = {str(k) : v for k,v in overide.items()}
        data = dict(data.items() + overide.items())

        return data

