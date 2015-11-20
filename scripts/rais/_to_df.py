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
db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"),
                     user=os.environ["DATAVIVA_DB_USER"],
                     passwd=os.environ["DATAVIVA_DB_PW"],
                     db=os.environ["DATAVIVA_DB_NAME"])
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
                        "emp_id":str, "est_id": str, "age": convertint, "wage": float}
        rais_df = pd.read_csv(input_file, header=0, sep=delim, names=cols, converters=coerce_cols, engine='c', decimal=',')
        rais_df = rais_df[["year", "bra_id", "cnae_id", "cbo_id", "wage", "num_emp", "est_id", "age"]]

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

        print (time.time() - s) / 60.0, "minutes to read."

    return rais_df
