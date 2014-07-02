def shard(ymbpw):
    
    ymb = ymbpw.groupby(level=['year','month','bra_id']).sum()
    ymbp = ymbpw.groupby(level=['year','month','bra_id','hs_id']).sum()
    ymbw = ymbpw.groupby(level=['year','month','bra_id','wld_id']).sum()
    ymp = ymbpw.groupby(level=['year','month','hs_id']).sum()
    ympw = ymbpw.groupby(level=['year','month','hs_id','wld_id']).sum()
    ymw = ymbpw.groupby(level=['year','month','wld_id']).sum()
    
    return [ymb, ymbp, ymbw, ymp, ympw, ymw]