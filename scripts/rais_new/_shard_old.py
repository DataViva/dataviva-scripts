import itertools, sys
import pandas as pd
def shard(ybio, raw):
    raw = raw[raw["wage"] > 0]
    
    yb = ybio.groupby(level=['year','bra_id']).sum()
    yi = ybio.groupby(level=['year','cnae_id']).sum()
    yo = ybio.groupby(level=['year','cbo_id']).sum()
    ybi = ybio.groupby(level=['year','bra_id','cnae_id']).sum()
    ybo = ybio.groupby(level=['year','bra_id','cbo_id']).sum()
    yio = ybio.groupby(level=['year','cnae_id','cbo_id']).sum()
    
    tbls = {"yb": yb, "yi": yi, "yo": yo, "ybi": ybi, "ybio": ybio, "ybo": ybo, "yio": yio}
    # tbls = {"yio":yio}
    lookup = {"b":"bra_id", "i":"cnae_id", "o":"cbo_id"}
    nestings = {"b":[2,4,8], "i":[1,3,6], "o":[1,2,4]}
    for t_name, t in tbls.items():
        print t_name
        temp = pd.DataFrame()
        my_nesting = [nestings[i] for i in t_name if i is not "y"]
        my_nesting_cols = [lookup[i] for i in t_name if i is not "y"]
        for depths in itertools.product(*my_nesting):
            my_raw = raw.copy()
            print depths
            for col_name, d in zip(my_nesting_cols, depths):
                my_raw[col_name] = my_raw[col_name].apply(lambda x: x[:d])
            temp = pd.concat([temp, my_raw.groupby(["year"]+my_nesting_cols).agg({"age": pd.Series.median, "wage": pd.Series.median})])
        # print temp.head(10)
        # print temp.index.is_unique, t.index.is_unique
        t["age_med"] = temp["age"]
        t["wage_med"] = temp["wage"]
        # t["num_emp_est"] = t["num_emp"].astype(float) / t["num_est"].astype(float)
        # t["wage_avg"] = t["wage"].astype(float) / t["num_emp"].astype(float)
        # t["age"] = t["age"].astype(float) / t["num_emp"].astype(float)
    return tbls