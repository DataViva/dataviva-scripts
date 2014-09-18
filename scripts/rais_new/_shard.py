import itertools, sys
import pandas as pd

from _aggregate import agg_rules

def shard(ybio, raw):
    raw = raw[raw["wage"] > 0]
    
    ybio_raw = ybio.reset_index()

    ybi = ybio_raw.groupby(['year','bra_id','cnae_id']).agg(agg_rules)
    ybo = ybio_raw.groupby(['year','bra_id','cbo_id']).agg(agg_rules)
    yio = ybio_raw.groupby(['year','cnae_id','cbo_id']).agg(agg_rules)

    yb = ybio_raw.groupby(['year','bra_id']).agg(agg_rules)
    yi = ybio_raw.groupby(['year','cnae_id']).agg(agg_rules)
    yo = ybio_raw.groupby(['year','cbo_id']).agg(agg_rules)
    
    tbls = {"yb": yb, "yi": yi, "yo": yo, "ybi": ybi, "ybio": ybio, "ybo": ybo, "yio": yio}
    # # tbls = {"yio":yio}
    # lookup = {"b":"bra_id", "i":"cnae_id", "o":"cbo_id"}
    # nestings = {"b":[8,2], "i":[5,1], "o":[4,1]}

    # for t_name, t in tbls.items():
    #     my_nesting = [nestings[i] for i in t_name if i is not "y"]
    #     my_nesting_cols = [lookup[i] for i in t_name if i is not "y"]
        
    #     for depths in itertools.product(*my_nesting):
    #         my_raw = raw.copy()
    #         print depths
    #         for col_name, d in zip(my_nesting_cols, depths):
    #             my_raw[col_name] = my_raw[col_name][:d]

            # temp = pd.concat([my_raw.groupby(["year"]+my_nesting_cols).agg({"age": pd.Series.median, "wage": pd.Series.median, "literacy" : lambda x: x.value_counts().idxmax() })])
# 
        # t["age_med"] = temp["age"]
        # t["wage_med"] = temp["wage"]

    return tbls