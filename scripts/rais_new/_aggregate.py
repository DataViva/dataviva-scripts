import sys
import pandas as pd
import numpy as np

def aggregate(rais_df):
    rais_df["cbo"] = rais_df["cbo"].apply(lambda x: str(x)[:4])
    ybio = rais_df.groupby(["year", "munic", "cnae", "cbo"]) \
            .agg({"wage": np.sum, "emp_id": pd.Series.nunique, "est_id": pd.Series.nunique})
    # print ybio.index.is_unique
    '''
        BRA AGGREGATIONS
    '''
    ybio_state = ybio.reset_index()
    ybio_state["munic"] = ybio_state["munic"].apply(lambda x: x[:2])
    ybio_state = ybio_state.groupby(["year", "munic", "cnae", "cbo"]).sum()

    ybio_meso = ybio.reset_index()
    ybio_meso["munic"] = ybio_meso["munic"].apply(lambda x: x[:4])
    ybio_meso = ybio_meso.groupby(["year", "munic", "cnae", "cbo"]).sum()
   
    ybio = pd.concat([ybio, ybio_state, ybio_meso])
    # print ybio.index.is_unique
    '''
       CNAE AGGREGATIONS
    '''
    ybio_cnae2 = ybio.reset_index()
    ybio_cnae2["cnae"] = ybio_cnae2["cnae"].apply(lambda x: str(x)[:2])
    ybio_cnae2 = ybio_cnae2.groupby(["year", "munic", "cnae", "cbo"]).sum()

    ybio = pd.concat([ybio, ybio_cnae2])
    # print ybio.index.is_unique
    '''
       CBO AGGREGATIONS
    '''
    ybio_cbo1 = ybio.reset_index()
    ybio_cbo1["cbo"] = ybio_cbo1["cbo"].apply(lambda x: str(x)[:1])
    ybio_cbo1 = ybio_cbo1.groupby(["year", "munic", "cnae", "cbo"]).sum()
    
    ybio_cbo2 = ybio.reset_index()
    ybio_cbo2["cbo"] = ybio_cbo2["cbo"].apply(lambda x: str(x)[:2])
    ybio_cbo2 = ybio_cbo2.groupby(["year", "munic", "cnae", "cbo"]).sum()
    
    ybio = pd.concat([ybio, ybio_cbo1, ybio_cbo2])
    # print ybio.index.is_unique
    
    ybio.index.names = ["year", "bra_id", "cnae_id", "cbo_id"]
    ybio.columns = ["wage", "num_emp", "num_est"]
    ybio = ybio[["wage", "num_emp", "num_est"]]
    
    ybio["num_emp_est"] = ybio["num_emp"].astype(float) / ybio["num_est"].astype(float)
    ybio["wage_avg"] = ybio["wage"].astype(float) / ybio["num_emp"].astype(float)
    
    # print ybio.index.is_unique
    ybio = ybio.sortlevel()
    
    # print ybio.index.is_unique
    
    # print ybio.xs([2002, "ac000000", "0139399", "7151"])
    # print ybio.xs([2002, "al020303", "4781400", "47"])
    
    return ybio
    