import sys
import os
import bz2
import MySQLdb
import pandas as pd
import numpy as np
from collections import defaultdict

'''
0:  Year
1:  Enroll_ID
2:  Studant_ID
3:  Age
4:  Gender
5:  Color
6:  Education_Mode
7:  Education_Level
8:  Education_Level_New
9:  Education
10: Class_ID
11: Course_ID
12: School_ID
13: Municipality
14: Location
15: Adm_Dependency (federal, state or municipal)
'''

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), user=os.environ[
                     "DATAVIVA_DB_USER"], passwd=os.environ["DATAVIVA_DB_PW"], db=os.environ["DATAVIVA_DB_NAME"])
cursor = db.cursor()

missing = {
    "bra_id": defaultdict(int),
    "school_id": defaultdict(int),
    "course_sc_id": defaultdict(int)
}

cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 9;")
bra_lookup = {str(r[0]): r[1] for r in cursor.fetchall()}

cursor.execute("select id from attrs_school;")
school_lookup = {str(r[0]): str(r[0]) for r in cursor.fetchall()}

cursor.execute("select id from attrs_course_sc;")
course_lookup = {str(r[0]): str(r[0]) for r in cursor.fetchall()}

BASIC_EDU_CODE = 'xx'

proper_age_map = {
    "xx002": 6 + 2,
    "xx003": 7 + 2,
    "xx004": 8 + 2,
    "xx005": 9 + 2,
    "xx006": 10 + 2,
    "xx007": 11 + 2,
    "xx008": 12 + 2,
    "xx009": 13 + 2,
    "xx010": 14 + 2,
    "xx011": 15 + 2,
    "xx012": 16 + 2,
    "xx013": 17 + 2,
    "xx014": 18 + 2,
    "xx016": 15 + 2,
    "xx017": 16 + 2,
    "xx018": 17 + 2,
    "xx019": 18 + 2,
}


def floatvert(x):
    x = x.replace(',', '.')
    try:
        return float(x)
    except:
        return np.nan


def bra_replace(raw):
    try:
        return bra_lookup[str(raw).strip()]
    except:
        missing["bra_id"][raw] += 1
        return None


def school_replace(raw):
    try:
        return school_lookup[str(raw).strip()]
    except:
        missing["school_id"][raw] += 1
        return None


def course_replace(raw):
    try:
        return course_lookup[str(raw).strip().zfill(5)]
    except:
        return BASIC_EDU_CODE  # -- if missing give BASIC edu code


def to_df(input_file_path, index=False, debug=False):
    if "bz2" in input_file_path:
        input_file = bz2.BZ2File(input_file_path)
    else:
        input_file = open(input_file_path, "rU")

    cols = ["year", "enroll_id", "student_id", "age", "gender", "color", "edu_mode",
            "edu_level", "edu_level_new", "edu", "class_id", "course_sc_id", "school_id",
            "bra_id_lives", "location_lives", "bra_id", "loc", "school_type"]
    delim = ";"
    coerce_cols = {"bra_id": bra_replace, "bra_id_lives": bra_replace, "school_id": school_replace,
                   "course_sc_id": course_replace}
    df = pd.read_csv(input_file, header=0, sep=delim, names=cols, converters=coerce_cols)
    df = df[["year", "enroll_id", "edu_level_new", "school_id",
             "course_sc_id", "class_id", "bra_id", "age", "bra_id_lives"]]

    # print df.course_sc_id.unique()
    # sys.exit()

    for col, missings in missing.items():
        if not len(missings):
            continue
        num_rows = df.shape[0]
        print
        print "[WARNING]"
        print "The following {0} IDs are not in the DB and will be dropped from the data.".format(col)
        print list(missings)
        # drop_criterion = rais_df[col].map(lambda x: x not in vals)
        # rais_df = rais_df[drop_criterion]
        df = df.dropna(subset=[col])
        print
        print "{0} rows deleted.".format(num_rows - df.shape[0])
        print

    print "Calculating Course IDs for basic education..."
    df.loc[df['course_sc_id'] == BASIC_EDU_CODE, 'course_sc_id'] = df[
        'course_sc_id'] + df.edu_level_new.astype(str).str.pad(3)
    df['course_sc_id'] = df['course_sc_id'].str.replace(' ', '0')

    print "Calculating proper age..."

    df["distorted_age"] = df.course_sc_id.map(proper_age_map)
    df.loc[df['distorted_age'].notnull(), 'distorted_age'] = (df.age >= df.distorted_age).astype(int)

    return df
