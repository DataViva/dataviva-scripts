def shard(ybio):
    
    yb = ybio.groupby(level=['year','bra_id']).sum()
    yi = ybio.groupby(level=['year','cnae_id']).sum()
    yo = ybio.groupby(level=['year','cbo_id']).sum()
    ybi = ybio.groupby(level=['year','bra_id','cnae_id']).sum()
    ybo = ybio.groupby(level=['year','bra_id','cbo_id']).sum()
    yio = ybio.groupby(level=['year','cnae_id','cbo_id']).sum()
    
    tbls = [yb, yi, yo, ybi, ybo, yio, ybio]
    for t in tbls:
        t["num_emp_est"] = t["num_emp"].astype(float) / t["num_est"].astype(float)
        t["wage_avg"] = t["wage"].astype(float) / t["num_emp"].astype(float)
        t["age"] = t["age"].asfloat(float) / t["num_emp"].astype(float)
    return tbls