import os, MySQLdb
import pandas as pd

db = MySQLdb.connect(host=os.environ.get("DATAVIVA2_DB_HOST", "localhost"), 
                     user=os.environ["DATAVIVA2_DB_USER"], 
                     passwd=os.environ["DATAVIVA2_DB_PW"], 
                     db=os.environ["DATAVIVA2_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

years = range(2007,2014)
cols = ["year", "enroll_id",  "student_id", "age", "gender", "color", "edu_mode", \
        "edu_level", "edu_level_new", "edu", "class_id", "course_sc_id", "school_id", \
        "bra_id_lives", "location_lives", "bra_id", "loc", "school_type"]
delim = ";"
usecols = ["school_id", "school_type"]
sc = pd.DataFrame()

for year in years:
    print year
    path = 'data/sc/School_census_{0}.csv'.format(year)
    frame = pd.read_csv(path, header=0, names=cols, usecols=usecols, sep=delim)
    sc = sc.append(frame, ignore_index=True)

sc["count"] = 1
sc = sc.groupby(["school_id", "school_type"]).count()
sc = sc["count"].unstack(level=-1)
for s, st in sc.iterrows():
    # print s, st.idxmax()
    cursor.execute("update attrs_school set school_type=%s where id=%s", [st.idxmax(), s])