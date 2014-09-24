import sys
import pandas as pd
import numpy as np
import itertools

agg_rules = {"wage": np.sum, 
             "num_jobs": np.sum,
             "num_est": pd.Series.count,
}            

median_rules = {
    "wage_med" : pd.Series.median,
    "age_med" : pd.Series.median,
    "edu_mode" : pd.Series.mode,
    "num_emp" : pd.Series.nunique
}

median_rules_d = {"wage_med" : pd.Series.median, "age_med" : pd.Series.median, "num_emp" : pd.Series.nunique} #, "num_jobs": np.sum}


agg_rules_d = { "wage": np.sum, "num_est": pd.Series.count, "num_jobs": np.sum }            

joint = dict(median_rules_d.items() + agg_rules_d.items() )

def replace_at_pos(x, dseq):
    d_id = list(x)
    i = 0
    while i < len(dseq):
        if dseq[i] == "0":
            d_id[i] = "0"
        i+=1
    return "".join(d_id)

def strmask(x,d):
    i = 0
    tmp = list(x)
    while i < len(tmp):
        if i != d:
            tmp[i] = "0"
        i+=1
    return "".join(tmp)

def medians_dempgraphics(rais_df, t_name):
    my_raw = rais_df.copy()
    mynewtable = pd.DataFrame() 

    all_demo_zeroes = ["1000", "0100", "0010", "0001"]
    

    lookup = {"b":"bra_id", "i":"cnae_id", "o":"cbo_id", "d": "d_id"}
    pk = ["year"] + [lookup[l] for l in t_name if l is not "y"]
    
    nestings = {"b":[8,2], "i":[6,1], "o":[4, 1], "d": [0,1,2,3]}

    my_nesting = [nestings[i] for i in t_name if i is not "y"]
    my_nesting_cols = [lookup[i] for i in t_name if i is not "y"]

    for depths in itertools.product(*my_nesting):
        print "Processing", my_nesting_cols, "at depths", depths
        my_raw["bra_id"] = rais_df["bra_id"].copy()
        my_raw["cnae_id"] = rais_df["cnae_id"].copy()
        my_raw["cbo_id"] = rais_df["cbo_id"].copy()
        my_raw["d_id"] = rais_df["d_id"].copy()

        for col_name, d in zip(my_nesting_cols, depths):
            if col_name == "d_id":
                my_raw[col_name] = my_raw[col_name].str.get(d)
            else:
                my_raw[col_name] = my_raw[col_name].str.slice(0, d)

        moi = my_raw.groupby(pk).agg( joint )
        # print moi.head()
        mynewtable = pd.concat([mynewtable, moi])
        print "done ", depths , " table"



    return mynewtable
    


def aggregate_demographics(rais_df):
    rais_df['wage_med'] = rais_df['wage']
    rais_df["age_med"] = rais_df["age"]
    rais_df["edu_mode"] = rais_df["literacy"]
    rais_df['num_jobs'] = 1

    rais_df = rais_df.rename(columns = {"est_id":"num_est"})
    rais_df = rais_df.drop(["color", "est_size"], axis=1)

    print "Aggregating demographic tables, from raw data..."
    dtables = ["ybid", "ybod", "ybd", "yod", "yid", "yd"]
    dtbls = {}
    for t_name in dtables:
        table = medians_dempgraphics(rais_df, t_name)
        dtbls[t_name] = table
    return dtbls

def aggregate(rais_df):
    rais_df['wage_med'] = rais_df['wage']
    rais_df['num_jobs'] = 1

    rais_df['wage_m'] = rais_df['wage'] * rais_df['gender']
    rais_df['wage_f'] = rais_df['wage'] * ((rais_df['gender']+1)%2)
    
    rais_df = rais_df.rename(columns = {"gender":"num_emp_m", "est_id":"num_est"})

    rais_df = rais_df.drop(["color", "est_size"], axis=1)
    pk = ["year", "bra_id", "cnae_id", "cbo_id"]
    ybio = rais_df.groupby(pk).agg(agg_rules)
    # print ybio.index.is_unique
    '''
        BRA AGGREGATIONS
    '''
    ybio_state = ybio.reset_index()
    ybio_state["bra_id"] = ybio_state["bra_id"].str.slice(0, 2)
    ybio_state = ybio_state.groupby(pk).agg(agg_rules)

    ybio = pd.concat([ybio, ybio_state])
    # print ybio.index.is_unique
    '''
       CNAE AGGREGATIONS
    '''
    ybio_cnae1 = ybio.reset_index()
    ybio_cnae1["cnae_id"] = ybio_cnae1["cnae_id"].str.get(0)
    ybio_cnae1 = ybio_cnae1.groupby(pk).agg(agg_rules)

    ybio = pd.concat([ybio, ybio_cnae1])
    # print ybio.index.is_unique
    '''
       CBO AGGREGATIONS
    '''
    ybio_cbo1 = ybio.reset_index()
    ybio_cbo1["cbo_id"] = ybio_cbo1["cbo_id"].str.get(0)
    ybio_cbo1 = ybio_cbo1.groupby(pk).agg(agg_rules)
        
    ybio = pd.concat([ybio, ybio_cbo1])
    
    ybio = ybio.sortlevel()
    
    # print ybio.xs([2002, "ac000000", "0139399", "7151"])
    # print ybio.xs([2002, "al020303", "4781400", "47"])
    
    return ybio
    

