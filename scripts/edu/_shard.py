def shard(ybsc):
    
    yb = ybsc.groupby(level=['year','bra_id']).sum()
    ys = ybsc.groupby(level=['year','school_id']).sum()
    yc = ybsc.groupby(level=['year','course_id']).sum()
    ybs = ybsc.groupby(level=['year','bra_id','school_id']).sum()
    ybc = ybsc.groupby(level=['year','bra_id','course_id']).sum()
    ysc = ybsc.groupby(level=['year','school_id','course_id']).sum()
    
    tbls = [yb, ys, yc, ybs, ybc, ysc, ybsc]
    for t in tbls:
        t["age"] = t["age"].astype(float) / t["enrolled"]
        t["age_m"] = t["age_m"].astype(float) / t["enrolled_m"]
        t["age_f"] = t["age_f"].astype(float) / t["enrolled_f"]
        
    return tbls