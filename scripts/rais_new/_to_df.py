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


def floatvert(x):
    x = x.replace(',', '.')
    try:
        return float(x)
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
        orig_cols = ['BrazilianOcupation_ID', 'EconomicAtivity_ID_CNAE', 'Literacy', 'Age', 'Establishment_ID', 'Simple', 'Municipality_ID', 'Employee_ID', 'Color', 'WageReceived', 'AverageMonthlyWage', 'Gender', 'Establishment_Size', 'Year', 'Establishment_ID_len']
        cols = ["cbo", "cnae", "literacy", "age", "est_id", "simple", "munic", "emp_id", "color", "wage_dec", "wage", "gender", "est_size", "year"]
        delim = ";"
        coerce_cols = {"cbo":str, "cnae":lambda x: str(x).strip(), \
                        "wage":floatvert, "emp_id":str, "est_id": str}
        rais_df = pd.read_csv(input_file, header=1, sep=delim, names=cols, converters=coerce_cols)
        rais_df = rais_df[["year", "munic", "cnae", "cbo", "wage", "emp_id", "est_id", "age", "color", "gender", "est_size"]]
        
        # rais_df = rais_df.dropna(how="any", subset=["wage", "emp_id", "est_id"])
        
        # print; print rais_df["year"].unique(); print;
        # rais_df["year"] = rais_df["year"].astype(int)
    
    return rais_df
