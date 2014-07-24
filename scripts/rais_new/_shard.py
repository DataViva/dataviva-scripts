def shard(ybio):
    
    yb = ybio.groupby(level=['year','bra_id']).sum()
    ybi = ybio.groupby(level=['year','bra_id','cnae_id']).sum()
    ybo = ybio.groupby(level=['year','bra_id','cbo_id']).sum()
    yi = ybio.groupby(level=['year','cnae_id']).sum()
    yio = ybio.groupby(level=['year','cnae_id','cbo_id']).sum()
    yo = ybio.groupby(level=['year','cbo_id']).sum()
    
    return [yb, ybi, ybo, yi, yio, yo]