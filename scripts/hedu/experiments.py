import os, sys, MySQLdb
import pandas as pd

file_path = os.path.dirname(os.path.realpath(__file__))
growth_lib_path = os.path.abspath(os.path.join(file_path, "..", "growth_lib"))
sys.path.insert(0, growth_lib_path)
import growth

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA2_DB_USER"], 
                        passwd=os.environ["DATAVIVA2_DB_PW"], 
                        db=os.environ["DATAVIVA2_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

# munic_totals = pd.read_sql("select bra_id, sum(enrolled) as enrolled from hedu_ybuc where year = 2012 group by year, bra_id", db, index_col="bra_id")
#
# df2 = pd.read_sql("select bra_id, name_en as course_name, sum(enrolled) as enrolled_by_course from hedu_ybuc, attrs_course_hedu where year = 2012 and bra_id = 'mg030000' and id = course_id group by year, bra_id, course_id", db, index_col="bra_id")
# print df2.head()

# totals = pd.read_sql("select course_id, sum(enrolled) as enrolled from hedu_ybuc where year = 2012 group by year, course_id", db, index_col="course_id")
# print totals.head()

# course_id = "811G01"
# df = pd.read_sql("select year, bra_id, name_en, sum(enrolled) as enrolled from hedu_ybuc, attrs_bra where year = 2012 and course_id = %s and id = bra_id group by year, bra_id", db, params=[course_id], index_col="bra_id")
# df["share"] = df["enrolled"] / munic_totals["enrolled"]
# print df.sort("share", ascending=False).head()

rcas = pd.read_sql("select bra_id, course_id, enrolled from hedu_ybuc where year =  2012 group by year, bra_id, course_id", db)
rcas = rcas.pivot(index="bra_id", columns="course_id", values="enrolled")
# rcas = rcas.pivot(index="course_id", columns="bra_id", values="enrolled")
rcas = growth.rca(rcas)
rcas = pd.DataFrame(rcas.stack(), columns=["enrolled_rca"])

course_names = pd.read_sql("select id as course_id, name_en from attrs_course_hedu", db, index_col="course_id")
# print course_names.head()

# print rcas.ix["212M02"].sort('enrolled_rca', ascending=False).head(10)
# sys.exit()

munic = rcas.ix["sp120509"]
munic["course_name"] = course_names["name_en"]

print munic.sort("enrolled_rca", ascending=False).head(10)