import sys, os, bz2
import pandas as pd
import numpy as np
import os, MySQLdb
from collections import defaultdict

file_path = os.path.dirname(os.path.realpath(__file__))
growth_lib_path = os.path.abspath(os.path.join(file_path, "..", "common"))
sys.path.insert(0, growth_lib_path)

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"], passwd=os.environ["DATAVIVA_DB_PW"], db=os.environ["DATAVIVA_DB_NAME"])
cursor = db.cursor()

missing = {
    "bra_id": defaultdict(int),
    "university_id": defaultdict(int),
    "course_hedu_id": defaultdict(int)
}

cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 9;")
bra_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}

cursor.execute("select id from attrs_university;")
university_lookup = {str(r[0]):str(r[0]) for r in cursor.fetchall()}

cursor.execute("select id from attrs_course_hedu;")
course_lookup = {str(r[0]):str(r[0]) for r in cursor.fetchall()}

def map_gender(x):
    MALE, FEMALE = 0, 1
    gender_dict = {MALE: 'A', FEMALE: 'B'}
    try: return str(gender_dict[int(x)])
    except: print x; sys.exit()

def map_color(color):
    WHITE=1
    BLACK=2
    MULTI=3
    ASIAN=4
    INDIAN=5
    UNIDENTIFIED = 6    
    UNKNOWN = 9
    color_dict = {UNIDENTIFIED: 'H' , INDIAN:'C', WHITE:'D', BLACK:'E', ASIAN:'F', MULTI:'G', 9:'H', -1:'H', 0:'H'}
    try: 
        return str(color_dict[int(color)])
    except Exception, e:
        raise Exception("Unknown color: error %s" % e)


def map_loc(loc):
    URBAN, RURAL = 1, 2
    loc_dict = {URBAN:'N', RURAL:'O'}
    try: return str(loc_dict[int(loc)])
    except: print loc; sys.exit()

def map_school_type(st):
    FEDERAL, STATE, LOCAL, PROFIT_PRIVATE, NONPROFIT_PRIVATE, SPECIAL = 1, 2, 3, 4, 5, 6
    loc_dict = {FEDERAL:'P', STATE:'Q', LOCAL:'R', PROFIT_PRIVATE:'S', NONPROFIT_PRIVATE:'T', SPECIAL:'U'}
    try: return str(loc_dict[int(st)])
    except: print st; sys.exit()

def floatvert(x):
    x = x.replace(',', '.')
    try: return float(x)
    except: return np.nan

def bra_replace(raw):
    try: return bra_lookup[str(raw).strip()]
    except: missing["bra_id"][raw] += 1; return "0xx000007"

def university_replace(raw):
    try: return university_lookup[str(raw).strip().zfill(5)]
    except: missing["university_id"][raw] += 1; return None

def course_replace(raw):
    try: return course_lookup[str(raw)]
    except: missing["course_hedu_id"][raw] += 1; return "000000"

def to_df(file_path, year):

    if "bz2" in file_path: input_file = bz2.BZ2File(file_path)
    else: input_file = open(file_path, "rU")
    
    cols = ["university_id","school_type","academic_organization","course_id_bad",\
            "degree","modality","level","student_id","enrolled","graduates","entrants",\
            "Year_entry","gender","age", "ethnicity", "bra_id","course_hedu_id","course_name",\
            "morning","afternoon","night", "full_time","year"]
    delim = ";"
    coerce_cols = {"bra_id":bra_replace, "university_id":university_replace, \
                    "course_hedu_id":course_replace, "ethnicity":map_color, "gender":map_gender, \
                    "school_type":map_school_type}
    df = pd.read_csv(input_file, header=0, sep=delim, names=cols, converters=coerce_cols)
    df = df.drop(["course_name","modality", "Year_entry","degree","course_id_bad","academic_organization","level"], axis=1)
    df = df[df["year"]==int(year)]
    
    for col, missings in missing.items():
        if not len(missings): continue
        num_rows = df.shape[0]
        print; print "[WARNING]"; print "The following {0} IDs are not in the DB.".format(col);
        print list(missings)
        # drop_criterion = rais_df[col].map(lambda x: x not in vals)
        # rais_df = rais_df[drop_criterion]
        # df = df.dropna(subset=[col])
        # print; print "{0} rows deleted.".format(num_rows - df.shape[0]); print;
    
    # print df.head()
    # sys.exit()

    return df
