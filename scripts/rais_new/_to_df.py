import sys, os, MySQLdb, time
import pandas as pd
import numpy as np
from collections import defaultdict
# from ..helpers import get_file

file_path = os.path.dirname(os.path.realpath(__file__))
utils_path = os.path.abspath(os.path.join(file_path, ".."))
sys.path.insert(0, utils_path)
from helpers import get_file

'''
0 Municipality_ID
1 EconmicAtivity_ID_ISIC
2 EconomicActivity_ID_CNAE
3 BrazilianOcupation_ID
4 AverageMonthlyWage
5 WageReceived
6 Employee_ID
7 Establishment_ID
8 Year
'''

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ["DATAVIVA2_DB_HOST"], user=os.environ["DATAVIVA2_DB_USER"], 
                        passwd=os.environ["DATAVIVA2_DB_PW"], 
                        db=os.environ["DATAVIVA2_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 9;")
bra_lookup = {str(r[0])[:-1]:r[1] for r in cursor.fetchall()}
bra_lookup["431454"] = "5rs030014"

cursor.execute("select substr(id, 2), id from attrs_cnae where length(id) = 6;")
cnae_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}

cursor.execute("select id, id from attrs_cbo where length(id) = 4;")
cbo_lookup = {r[0]:r[1] for r in cursor.fetchall()}
cbo_lookup["-1"] = "xxxx" # data uses -1 for missing occupation

missing = {
    "bra_id": defaultdict(int),
    "cnae_id": defaultdict(int),
    "cbo_id": defaultdict(int)
}

def bra_replace(raw):
    try:
        return bra_lookup[str(raw).strip()]
    except:
        missing["bra_id"][raw] += 1
        return None

def cnae_replace(raw):
    try:
        return cnae_lookup[str(raw).strip()]
    except:
        missing["cnae_id"][raw] += 1
        return None

def cbo_replace(raw):
    try:
        return cbo_lookup[str(raw).strip()[:4]]
    except:
        missing["cbo_id"][raw] += 1
        return None

def convertint(x):
    if not x:
        return -999
    elif "\r" in str(x):
        return -999
    return int(x)

def to_df(input_file_path, index=False):
    input_file = get_file(input_file_path)
    s = time.time()
    
    if index:
        index_lookup = {"y":"year", "b":"bra_id", "i":"cnae_id", "o":"cbo_id", "d": "d_id"}
        index_cols = [index_lookup[i] for i in index]
        rais_df = pd.read_csv(input_file, sep="\t", converters={"bra_id":str, "cbo_id":str, "cnae_id":str})
        rais_df = rais_df.set_index(index_cols)
    else:
        orig_cols = ['BrazilianOcupation_ID', 'EconomicAtivity_ID_CNAE', 'Literacy', 'Age', 'Establishment_ID', 'Simple', 'Municipality_ID', 'Employee_ID', 'Color', 'WageReceived', 'AverageMonthlyWage', 'Gender', 'Establishment_Size', 'Year', 'Establishment_ID_len']
        cols = ["cbo_id", "cnae_id", "literacy", "age", "est_id", "simple", "bra_id", "num_emp", "color", "wage_dec", "wage", "gender", "est_size", "year"]
        delim = ";"
        coerce_cols = {"bra_id": bra_replace, "cnae_id":cnae_replace, "cbo_id":cbo_replace, \
                        "emp_id":str, "est_id": str, "age": convertint}
        rais_df = pd.read_csv(input_file, header=0, sep=delim, names=cols, converters=coerce_cols, engine='c', decimal=',')
        rais_df = rais_df[["year", "bra_id", "cnae_id", "cbo_id", "wage", "num_emp", "est_id", "age", "color", "gender", "est_size", "literacy"]]

        print "first remove rows with empty ages, if any..."
        count = rais_df[ rais_df.age == -999 ].age.count()
        if count > 0:
            print "** REMOVED", count, "rows due to empty ages"
        rais_df = rais_df[ rais_df.age != -999 ]
        
        print "finding missing attrs..."
        for col, missings in missing.items():
            if not len(missings): continue
            num_rows = rais_df.shape[0]
            print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(col);
            print list(missings)
            # drop_criterion = rais_df[col].map(lambda x: x not in vals)
            # rais_df = rais_df[drop_criterion]
            rais_df = rais_df.dropna(subset=[col])
            print; print "{0} rows deleted.".format(num_rows - rais_df.shape[0]); print;

        print "generating demographic codes..."
        FEMALE, MALE = 0, 1
        gender_dict = {MALE: 'A', FEMALE: 'B'}
        rais_df["gender"] = rais_df["gender"].replace(gender_dict)
        
        INDIAN, WHITE, BLACK, ASIAN, MULTI, UNKNOWN = 1,2,4,6,8,9
        color_dict = {INDIAN:'C', WHITE:'D', BLACK:'E', ASIAN:'F', MULTI:'G', UNKNOWN:'H', -1:'H' }
        rais_df["color"] = rais_df["color"].replace(color_dict)
        
        lit_dict = {1:'I', 2:'I', 3:'J', 4:'J', 5:'J', 6:'J', 7:'K', 8:'K', 9:'L', -1:'M'}
        rais_df["literacy"] = rais_df["literacy"].replace(lit_dict)
        
        rais_df["age_demo"] = (rais_df["age"] / 10).astype(int)
        rais_df["age_demo"] = rais_df["age_demo"].clip(1,6)
        
        rais_df["d_id"] = rais_df['gender'].str.cat([rais_df['age_demo'].values.astype(str), rais_df['color'].values.astype(str), rais_df['literacy'].values.astype(str)])

        rais_df = rais_df.drop(["gender", "color", "age_demo", "literacy"], axis=1)
                
        # rais_df["new_est_size"] = rais_df["cnae_id"].str.slice(1, 3).astype(int)
        # rais_df["new_est_size"][rais_df["new_est_size"].between(5, 35)] = -1
        # rais_df["new_est_size"][rais_df["new_est_size"] >= 0] = 0
        #
        # print rais_df["new_est_size"].mask(rais_df["new_est_size"] >= 0).head()
        # print rais_df["new_est_size"].where(rais_df["new_est_size"] >= 0).head()
        
        print "determining establishment sizes..."
        rais_df["new_est_size_1"] = rais_df["cnae_id"].str.slice(1, 3).astype(int)
        rais_df.loc[rais_df["new_est_size_1"].between(5, 35),"new_est_size_1"] = -1
        rais_df.loc[rais_df["new_est_size_1"] >= 0,"new_est_size_1"] = 0
        
        rais_df["new_est_size_2"] = rais_df["new_est_size_1"].mask(rais_df["new_est_size_1"] >= 0).head()
        rais_df["new_est_size_1"] = rais_df["new_est_size_1"].where(rais_df["new_est_size_1"] >= 0).head()

        rais_df.loc[rais_df["new_est_size_2"]==-1,"new_est_size_2"] = 0
        rais_df["new_est_size_2"] = rais_df["new_est_size_2"] + rais_df["est_size"]
        rais_df["new_est_size_1"] = rais_df["new_est_size_1"] + rais_df["est_size"]
        
        est_size_1_lookup = {1:0, 2:0, 3:1, 4:1, 5:2, 6:3, 7:3, 8:3, 9:3}
        est_size_2_lookup = {1:0, 2:0, 3:0, 4:1, 5:1, 6:2, 7:2, 8:3, 9:3}
        
        rais_df["new_est_size_1"] = rais_df["new_est_size_1"].replace(est_size_1_lookup)
        rais_df["new_est_size_2"] = rais_df["new_est_size_2"].replace(est_size_2_lookup)
        
        rais_df["est_size"] = rais_df["new_est_size_1"].fillna(0) + rais_df["new_est_size_2"].fillna(0)
        
        rais_df = rais_df.drop(["new_est_size_1", "new_est_size_2"], axis=1)
        
        print (time.time() - s) / 60.0, "minutes to read."

    return rais_df
