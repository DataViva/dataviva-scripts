import MySQLdb, sys, os
import pandas as pd
import pandas.io.sql as sql

db = MySQLdb.connect(host=os.environ["DATAVIVA2_DB_HOST"],
                     user=os.environ["DATAVIVA2_DB_USER"],
                     passwd=os.environ["DATAVIVA2_DB_PW"],
                     db=os.environ["DATAVIVA2_DB_NAME"])

q = "select bra_id, cbo_id, d_id, num_emp from rais_ybod where year = 2013 and bra_id_len = 9 and cbo_id_len = 4 and d_id in ('C', 'D', 'E', 'F', 'G', 'H')"
ybod = sql.read_sql(q, db)

ybod2 = ybod.set_index(["bra_id","cbo_id","d_id"])

ybod2 = ybod2["num_emp"].unstack(level=-1)

ybod2 = ybod2.rank(ascending=False).sum(axis=1).order(ascending=False)
