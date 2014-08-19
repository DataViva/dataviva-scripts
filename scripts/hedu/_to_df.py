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

def to_df(input_file_path, year):
    if "bz2" in input_file_path:
        input_file = bz2.BZ2File(input_file_path)
    else:
        input_file = open(input_file_path, "rU")
    
    cols = ["academic_organization","munic","university_id","year","adm_category","course_id","course_name","modality","level","openings","enrolled","graduates","entrants","degree"]
    delim = ","
    # coerce_cols = {"course_id":str, "class_id":str}
    coerce_cols = {"munic":str, "university_id":str}
    df = pd.read_csv(input_file, header=0, sep=delim, names=cols, converters=coerce_cols)
    df = df[["year", "munic", "university_id", "course_id", "openings", "enrolled", "graduates", "entrants"]]
    df = df[df["year"]==int(year)]
    
    return df
