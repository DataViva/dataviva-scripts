import sys, os, bz2
import pandas as pd
import numpy as np

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

def floatvert(x):
    x = x.replace(',', '.')
    try:
        return float(x)
    except:
        return np.nan


def to_df(input_file_path, index=False, debug=False):
    if "bz2" in input_file_path:
        input_file = bz2.BZ2File(input_file_path)
    else:
        input_file = open(input_file_path, "rU")
    
    # if index:
        # index_lookup = {"y":"year", "b":"bra_id", "i":"cnae_id", "o":"cbo_id"}
        # index_cols = [index_lookup[i] for i in index]
        # rais_df = pd.read_csv(input_file, sep="\t", converters={"cbo_id":str, "cnae_id":str})
        # rais_df = rais_df.set_index(index_cols)
    # else:
        #cols = ["year", "enroll_id", "student_id", "age", "gender", "color", "edu_mode", "edu_level", "edu_level_new", "edu",
         # "class_id", "course_id", "school_id", ]
    cols = ["year", "enroll_id",  "student_id", "age", "gender", "color", "edu_mode",  "edu_level", "edu_level_new", "edu", "class_id", "course_id", "school_id","munic_lives", "location_lives", "munic", "loc", "adm_dep"]
    delim = ";"
        # coerce_cols = {"course_id":str, "class_id":str}
    coerce_cols = {"course_id":str, "munic_lives": str}
    rais_df = pd.read_csv(input_file, header=0, sep=delim, names=cols, converters=coerce_cols)
    rais_df = rais_df[["year", "enroll_id", "gender", "color", "edu_level_new", "school_id", "class_id", "munic", "age", "loc", "munic_lives"]]

    return rais_df
