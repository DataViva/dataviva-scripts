import sys, os, bz2, MySQLdb
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
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA2_DB_USER"], passwd=os.environ["DATAVIVA2_DB_PW"], db=os.environ["DATAVIVA2_DB_NAME"])
cursor = db.cursor()

missing = {
    "bra_id": defaultdict(int),
    "school_id": defaultdict(int),
    "course_id": defaultdict(int)
}

cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 9;")
bra_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}

cursor.execute("select id from attrs_school;")
school_lookup = {str(r[0]):str(r[0]) for r in cursor.fetchall()}

cursor.execute("select id from attrs_course_sc;")
course_lookup = {str(r[0]):str(r[0]) for r in cursor.fetchall()}

BASIC_EDU_CODE = 'xx'


proper_age_map = {
    "xx002": 6 + 2,
    "xx003": 7 + 2,
    "xx004" : 8 + 2,
    "xx005" : 9 + 2,
    "xx006" : 10 + 2,
    "xx007" : 11 + 2,
    "xx008" : 12 + 2,
    "xx009" : 13 + 2,
    "xx010" : 14 + 2,
    "xx011" : 15 + 2,
    "xx012" : 16 + 2,
    "xx013" : 17 + 2,
}

def map_gender(x):
    MALE, FEMALE = 0, 1
    gender_dict = {MALE: 'A', FEMALE: 'B'}
    try: return str(gender_dict[int(x)])
    except: print x; sys.exit()

def map_color(color):
    INDIAN=5
    WHITE=1
    BLACK=2
    ASIAN=4
    MULTI=3
    UNKNOWN = 9
    color_dict = {INDIAN:'C', WHITE:'D', BLACK:'E', ASIAN:'F', MULTI:'G', 9:'H', -1:'H', 0:'H'}
    try: return str(color_dict[int(color)])
    except: print color; sys.exit()

def map_loc(loc):
    URBAN, RURAL = 1, 2
    loc_dict = {URBAN:'N', RURAL:'O'}
    try: return str(loc_dict[int(loc)])
    except: print loc; sys.exit()

def map_school_type(st):
    FEDERAL, STATE, LOCAL, PRIVATE = 1, 2, 3, 4
    loc_dict = {FEDERAL:'P', STATE:'Q', LOCAL:'R', PRIVATE:'S'}
    try: return str(loc_dict[int(st)])
    except: print st; sys.exit()

def floatvert(x):
    x = x.replace(',', '.')
    try: return float(x)
    except: return np.nan

def bra_replace(raw):
    try: return bra_lookup[str(raw).strip()]
    except: missing["bra_id"][raw] += 1; return None

def school_replace(raw):
    try: return school_lookup[str(raw).strip()]
    except: missing["school_id"][raw] += 1; return None

def course_replace(raw):
    try: return course_lookup[str(raw).strip()]
    except: return BASIC_EDU_CODE # -- if missing give BASIC edu code

def to_df(input_file_path, index=False, debug=False):
    if "bz2" in input_file_path:
        input_file = bz2.BZ2File(input_file_path)
    else:
        input_file = open(input_file_path, "rU")

    cols = ["year", "enroll_id",  "student_id", "age", "gender", "color", "edu_mode", \
            "edu_level", "edu_level_new", "edu", "class_id", "course_id", "school_id", \
            "bra_id_lives", "location_lives", "bra_id", "loc", "school_type"]
    delim = ";"
    coerce_cols = {"bra_id":bra_replace, "bra_id_lives":bra_replace, "school_id":school_replace, \
                    "course_id":course_replace, "color":map_color, "gender":map_gender, \
                    "loc":map_loc, "school_type":map_school_type}
    df = pd.read_csv(input_file, header=0, sep=delim, names=cols, converters=coerce_cols)
    df = df[["year", "enroll_id", "gender", "color", "edu_level_new", "school_id", "course_id", "class_id", "bra_id", "age", "loc", "bra_id_lives", "school_type"]]
    
    # print df.course_id.unique()
    # sys.exit()
    
    for col, missings in missing.items():
        if not len(missings): continue
        num_rows = df.shape[0]
        print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(col);
        print list(missings)
        # drop_criterion = rais_df[col].map(lambda x: x not in vals)
        # rais_df = rais_df[drop_criterion]
        df = df.dropna(subset=[col])
        print; print "{0} rows deleted.".format(num_rows - df.shape[0]); print;

    # df[df.course_id == BASIC_EDU_CODE] = BASIC_EDU_CODE[:-2] + str(df.edu_level_new).zfill(2)
    print "Calculating Course IDs for basic education..."
    # df.edu_level_new = df.edu_level_new.astype(str)
    # df["course_id"] = df.apply(lambda x: "xx" + x["edu_level_new"].zfill(3) if x["course_id"] == BASIC_EDU_CODE else x["course_id"], axis=1)
    df.loc[df['course_id'] == BASIC_EDU_CODE, 'course_id'] = df['course_id'] + df.edu_level_new.astype(str).str.pad(3)
    df['course_id'] = df['course_id'].str.replace(' ', '0')

    print "Calculating proper age..."

    df["distorted_age"] = df.course_id.map(proper_age_map)
    df.loc[df['distorted_age'].notnull() , 'distorted_age'] = (df.age >= df.distorted_age).astype(int) 
    
    return df
    
    # print df.head()
    # sys.exit()
    # print "generating demographic codes..."
    # # df["d_id"] = df.apply(lambda x:'%s%s%s%s' % (map_gender(x['gender']), map_color(x['color']), \
    # #                                             map_loc(x['loc']), map_school_type(x['school_type'])), axis=1)
    # # df = df.drop(["color", "gender", "edu_level_new", "loc", "bra_id_lives", "school_type"], axis=1)
    
    # df_dem = pd.DataFrame()
    # dems = ['gender', 'color', 'loc', 'school_type']
    # for dem in dems:
    #     this_df = df.copy()
    #     this_df["d_id"] = this_df[dem]
    #     this_df = this_df.drop(dems, axis=1)
    #     df_dem = df_dem.append(this_df)
    
    # # for col, missings in missing.items():
    # #     if not len(missings): continue
    # #     num_rows = df.shape[0]
    # #     print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(col);
    # #     print list(missings)
    # #     # drop_criterion = rais_df[col].map(lambda x: x not in vals)
    # #     # rais_df = rais_df[drop_criterion]
    # #     df = df.dropna(subset=[col])
    # #     print; print "{0} rows deleted.".format(num_rows - df.shape[0]); print;
    
    # # print df[df["course_id"].notnull()].head()

    # return df_dem
