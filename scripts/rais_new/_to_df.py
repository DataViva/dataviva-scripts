import sys, os
import pandas as pd
import numpy as np
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


def convert(x, d):
    try:
        int(x)
        d["int"] += 1
    except ValueError:
        try:
            float(x)
            d["float"] += 1
        except:
            if x in d:
                d[x] += 1
            else:
                d[x] = 1
    return d

def floatvert(x):
    x = x.replace(',', '.')
    try:
        return float(x)
    except:
        return np.nan

def intvert(x):
    try:
        return int(x)
    except:
        return np.nan


def to_df(input_file_path, index=False, debug=False):
    input_file = get_file(input_file_path)
    
    if index:
        index_lookup = {"y":"year", "b":"bra_id", "i":"cnae_id", "o":"cbo_id"}
        index_cols = [index_lookup[i] for i in index]
        rais_df = pd.read_csv(input_file, sep="\t", converters={"cbo_id":str, "cnae_id":str})
        rais_df = rais_df.set_index(index_cols)
    else:
        cols = ["munic", "isic", "cnae", "cbo", "wage", "wage_dec", "emp_id", "est_id", "year"]
        delim = ","
        
        
        # wage_vals = {"int":0, "float":0}
        # wage_avg_vals = {"int":0, "float":0}
        # import csv
        # with input_file as csvfile:
        #     reader = csv.reader(csvfile, delimiter=',')
        #     for row in reader:
        #         wage = row[5].replace(",", ".")
        #         wage_avg = row[5].replace(",", ".")
        #
        #         wage_vals = convert(wage, wage_vals)
        #         wage_avg_vals = convert(wage_avg, wage_avg_vals)
        #
        # print wage_vals
        # print
        # print
        # print wage_avg_vals
        # sys.exit()
        
        
        rais_df = pd.read_csv(input_file,header=1, sep=delim, names=cols, converters={"cbo":str, "cnae":lambda x: str(x).strip(), "wage":floatvert, "wage_dec": floatvert, "emp_id":str, "est_id": str})
        rais_df = rais_df[["year", "munic", "cnae", "cbo", "wage", "emp_id", "est_id"]]
        # print rais_df.shape
        
        rais_df = rais_df.dropna(how="any", subset=["wage", "emp_id", "est_id"])
        
        print; print rais_df["year"].unique(); print;
        
        rais_df["year"] = rais_df["year"].astype(int)

        # print rais_df.shape
        
        # rais_df["cnae"] = rais_df["cnae"].apply(lambda x: x.strip())
        # rais_df["wage"] = rais_df["wage"].astype(float)
        # rais_df["wage_avg"] = rais_df["wage_avg"].astype(float)
        # rais_df["emp_id"] = rais_df["emp_id"].astype(int)
        # rais_df["est_id"] = rais_df["est_id"].astype(int)
    
    return rais_df