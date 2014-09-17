import sys
import pandas as pd
import numpy as np

def aggregate(rais_df):
    rais_df = rais_df.drop(["age"], axis=1)
    
    rais_df['wage_m'] = rais_df['wage'] * rais_df['gender']
    rais_df['wage_f'] = rais_df['wage'] * ((rais_df['gender']+1)%2)
    
    rais_df = rais_df.rename(columns = {"gender":"num_emp_m", "emp_id":"num_emp", "est_id":"num_est"})

    rais_df = rais_df.drop(["color", "est_size"], axis=1)
    ybio = rais_df.groupby(["year", "bra_id", "cnae_id", "cbo_id"]) \
            .agg({"wage": np.sum, "num_emp": pd.Series.count, "num_est": pd.Series.count,\
                    "num_emp_m": np.sum, "wage_m": np.sum, "wage_f": np.sum})
    # print ybio.index.is_unique
    '''
        BRA AGGREGATIONS
    '''
    ybio_state = ybio.reset_index()
    ybio_state["bra_id"] = ybio_state["bra_id"].apply(lambda x: x[:2])
    ybio_state = ybio_state.groupby(["year", "bra_id", "cnae_id", "cbo_id"]).sum()
   
    ybio = pd.concat([ybio, ybio_state])
    # print ybio.index.is_unique
    '''
       CNAE AGGREGATIONS
    '''
    ybio_cnae1 = ybio.reset_index()
    ybio_cnae1["cnae_id"] = ybio_cnae1["cnae_id"].apply(lambda x: str(x)[:1])
    ybio_cnae1 = ybio_cnae1.groupby(["year", "bra_id", "cnae_id", "cbo_id"]).sum()
    
    ybio = pd.concat([ybio, ybio_cnae1])
    # print ybio.index.is_unique
    '''
       CBO AGGREGATIONS
    '''
    ybio_cbo1 = ybio.reset_index()
    ybio_cbo1["cbo_id"] = ybio_cbo1["cbo_id"].apply(lambda x: str(x)[:1])
    ybio_cbo1 = ybio_cbo1.groupby(["year", "bra_id", "cnae_id", "cbo_id"]).sum()
        
    ybio = pd.concat([ybio, ybio_cbo1])
    
    ybio = ybio.sortlevel()
    
    # print ybio.xs([2002, "ac000000", "0139399", "7151"])
    # print ybio.xs([2002, "al020303", "4781400", "47"])
    
    return ybio
    