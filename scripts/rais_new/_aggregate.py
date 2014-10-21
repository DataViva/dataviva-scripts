import sys, os
import pandas as pd
import numpy as np
import itertools
import bottleneck
import MySQLdb

agg_rules = {"wage": np.sum, 
             "num_jobs": np.sum,
}            

median_rules = {
    "wage_med" : bottleneck.median,
    "age_med" : bottleneck.median,
    "edu_mode" : bottleneck.median,
    "num_emp" : pd.Series.nunique,
    "num_est": pd.Series.nunique
}

median_rules_d = {"wage_med" :bottleneck.median, "age_med" : bottleneck.median, "num_emp" : pd.Series.nunique, "num_est": pd.Series.nunique, "edu_mode": bottleneck.median}
agg_rules_d = { "wage": np.sum, "num_jobs": np.sum }            

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

def medians_demographics(rais_df, t_name, geo_depths):
    geo_depths.reverse()
    mynewtable = pd.DataFrame() 

    all_demo_zeroes = ["1000", "0100", "0010", "0001"]
    

    lookup = {"b":"bra_id", "i":"cnae_id", "o":"cbo_id", "d": "d_id"}
    pk = ["year"] + [lookup[l] for l in t_name if l is not "y"]
    
    nestings = {"b": geo_depths, "i":[6,1], "o":[4, 1], "d": [0,1,2,3]}

    my_nesting = [nestings[i] for i in t_name if i is not "y"]
    my_nesting_cols = [lookup[i] for i in t_name if i is not "y"]

    for depths in itertools.product(*my_nesting):
        print "Processing", my_nesting_cols, "at depths", depths

        my_pk = [rais_df["year"]]

        for col_name, d in zip(my_nesting_cols, depths):
            if col_name == "d_id":
                my_pk.append( rais_df[col_name].str.get(d) )
            else:
                my_pk.append( rais_df[col_name].str.slice(0, d) )

        moi = rais_df.groupby(my_pk, sort=False).agg( joint )
        # print moi.head()
        mynewtable = pd.concat([mynewtable, moi])
        print "done ", depths , " table"



    return mynewtable
    
def get_planning_regions():
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    cursor.execute("select bra_id, pr_id from attrs_bra_pr")
    return {r[0]:r[1] for r in cursor.fetchall()}

def aggregate_demographics(rais_df, geo_depths):
    rais_df['wage_med'] = rais_df['wage']
    rais_df['num_jobs'] = 1

    rais_df = rais_df.rename(columns = {"age": "age_med", "literacy": "edu_mode"})
    rais_df = rais_df.drop(["color", "gender", "est_size"], axis=1)

    print "Aggregating demographic tables, from raw data..."
    dtables = ["ybid", "ybod", "ybd", "yod", "yid"]
    dtbls = {}
    for t_name in dtables:
        table = medians_demographics(rais_df, t_name, geo_depths)
        dtbls[t_name] = table
    return dtbls

def aggregate(rais_df, geo_depths):
    rais_df['wage_med'] = rais_df['wage']
    rais_df['num_jobs'] = 1

    # rais_df['wage_m'] = rais_df['wage'] * rais_df['gender']
    # rais_df['wage_f'] = rais_df['wage'] * ((rais_df['gender']+1)%2)

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
    
    ybio_meso = ybio.reset_index()
    ybio_meso["bra_id"] = ybio_meso["bra_id"].str.slice(0, 4)
    ybio_meso = ybio_meso.groupby(pk).agg(agg_rules)
    
    ybio_micro = ybio.reset_index()
    ybio_micro["bra_id"] = ybio_micro["bra_id"].str.slice(0, 6)
    ybio_micro = ybio_micro.groupby(pk).agg(agg_rules)
    
    ybio_pr = ybio.reset_index()
    ybio_pr = ybio_pr[ybio_pr["bra_id"].map(lambda x: x[:2] == "mg")]
    ybio_pr["bra_id"] = ybio_pr["bra_id"].astype(str).replace(get_planning_regions())
    ybio_pr = ybio_pr.groupby(pk).agg(agg_rules)

    ybio = pd.concat([ybio, ybio_state, ybio_meso, ybio_micro, ybio_pr])
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
    

