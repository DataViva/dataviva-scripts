import sys, os, bz2
import pandas as pd
import numpy as np
import os, MySQLdb


file_path = os.path.dirname(os.path.realpath(__file__))
growth_lib_path = os.path.abspath(os.path.join(file_path, "..", "common"))
sys.path.insert(0, growth_lib_path)

from demographics import map_gender, map_ethnicity, map_age

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


def gen_converters():
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA2_DB_USER"], 
                            passwd=os.environ["DATAVIVA2_DB_PW"], 
                            db=os.environ["DATAVIVA2_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 8;")
    bras = {str(r[0]):r[1] for r in cursor.fetchall()}
    
    
    def bra_lookup(x):
        if not x:
            return "xx000007"
        return bras[x]

    cursor.execute("select id, id from attrs_course_hedu;")
    courses = {str(r[0]):r[1] for r in cursor.fetchall()}

    def course_lookup(x):
        if not x:
            return "UNKNOWN"
        return courses[x]

    return bra_lookup, course_lookup

def to_df(input_file_path, year):

    bra_lookup, course_lookup = gen_converters()

    if "bz2" in input_file_path:
        input_file = bz2.BZ2File(input_file_path)
    else:
        input_file = open(input_file_path, "rU")
    
    # cols = ["academic_organization","munic","university_id","year","adm_category","course_id","course_name","modality","level","openings","enrolled","graduates","entrants","degree"]
    cols = ["university_id","adm_category","academic_organization","course_id_bad","degree","modality","level","student_id","enrolled_id","graduates","entrants","Year_entry","gender","age", "ethnicity", "bra_id","course_id","Name_Course","morning","afternoon","night", "full_time","year"]
    delim = ";"
    # coerce_cols = {"course_id":str, "class_id":str}
    coerce_cols = {"bra_id":bra_lookup, "course_id":course_lookup}
    df = pd.read_csv(input_file, header=0, sep=delim, names=cols, converters=coerce_cols)
    df = df[["year", "bra_id", "university_id", "course_id", "student_id", "enrolled_id", "graduates", "entrants", "gender", "age", "ethnicity", "level", "morning", "afternoon", "night", "full_time" ]]
    df = df[df["year"]==int(year)]

    print "Computing demographics..."
    df["d_id"] = df.apply(lambda x:'%s%s%s' % (map_gender(x['gender']), map_age(x['age']), map_ethnicity(x['ethnicity'])), axis=1)
    df = df[["year", "bra_id", "university_id", "course_id", "d_id", "graduates", "entrants", "full_time", "morning", "afternoon", "night" ]]
    df["enrolled"] = 1
    # tot = df['bra_id'].count()
    # missing_bra = df[(df["bra_id"] == 'xx000007') & (df["modality"] == 2) ].bra_id.count()
    # print "Total", tot, "Missing", missing_bra
    # print "pct:", float(1.0*missing_bra/tot*1.0)


    return df
