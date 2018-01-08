import os
import bz2
import MySQLdb
import pandas as pd
import numpy as np
from collections import defaultdict

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), user=os.environ[
                     "DATAVIVA_DB_USER"], passwd=os.environ["DATAVIVA_DB_PW"], db=os.environ["DATAVIVA_DB_NAME"])
cursor = db.cursor()

missing = {
    "bra_id": defaultdict(int),
    "school_id": defaultdict(int),
    "course_sc_id": defaultdict(int)
}

cursor.execute(
    "select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 9;")
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
        return course_lookup[str(raw).strip().zfill(5) if len(raw) > 0 else str(raw)]
    except:
        return BASIC_EDU_CODE  # -- if missing give BASIC edu code


def edu_level_replace(raw):
    return str(raw).zfill(3)


def to_df(file_path, indexes=None):
    if "bz2" in file_path:
        input_file = bz2.BZ2File(file_path)
    else:
        input_file = open(file_path, "rU")

    if indexes:
        converters = {"course_hedu_id": str, "school_id": str}
        df = pd.read_csv(
            input_file, sep="\t", converters=converters, engine='python')
        df = df.set_index(indexes)
    else:
        cols = ["year", "enroll_id", "student_id", "age", "gender", "color", "edu_mode",
                "edu_level", "edu_level_new", "edu", "class_id", "course_sc_id", "school_id",
                "bra_id_lives", "location_lives", "bra_id", "loc", "school_type"]
        delim = ";"
        coerce_cols = {"bra_id": bra_replace, "bra_id_lives": bra_replace, "school_id": school_replace,
                       "course_sc_id": course_replace, "edu_level_new": edu_level_replace}
        df = pd.read_csv(
            input_file, header=0, sep=delim, names=cols, converters=coerce_cols)
        df = df[["year", "enroll_id", "edu_level_new", "school_id",
                 "course_sc_id", "class_id", "bra_id", "age", "bra_id_lives"]]

        print "Calculating Course IDs for basic education..."

        df.loc[df['course_sc_id'] == BASIC_EDU_CODE, 'course_sc_id'] = df['course_sc_id'] + df.edu_level_new
        df['course_sc_id'] = df['course_sc_id'].str.replace(' ', '0')

        print "Calculating proper age..."
        df["distorted_age"] = df.course_sc_id.map(proper_age_map)
        df.loc[df['distorted_age'].notnull(), 'distorted_age'] = (df.age >= df.distorted_age).astype(int)

    for col, missings in missing.items():
        if not len(missings):
            continue
        num_rows = df.shape[0]
        print
        print "[WARNING]"
        print "The following {0} IDs are not in the DB. Total: ".format(col, num_rows)
        print list(missings)

    return df
