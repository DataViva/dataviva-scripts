def shard(ymbpw):
    ymbpw = ymbpw.reset_index()
    
    bra_criterion = ymbpw.bra_id.map(lambda x: len(x) == 9)
    hs_criterion = ymbpw.hs_id.map(lambda x: len(x) == 6)
    wld_criterion = ymbpw.wld_id.map(lambda x: len(x) == 5)
    
    ymb = ymbpw[wld_criterion & hs_criterion]
    ymb = ymb.groupby(['year','month','bra_id']).sum()
    
    ymbp = ymbpw[wld_criterion]
    ymbp = ymbp.groupby(['year','month','bra_id','hs_id']).sum()
    
    ymbw = ymbpw[hs_criterion]
    ymbw = ymbw.groupby(['year','month','bra_id','wld_id']).sum()
    
    ymp = ymbpw[bra_criterion & wld_criterion]
    ymp = ymp.groupby(['year','month','hs_id']).sum()
    
    ympw = ymbpw[bra_criterion]
    ympw = ympw.groupby(['year','month','hs_id','wld_id']).sum()
    
    ymw = ymbpw[bra_criterion & hs_criterion]
    ymw = ymw.groupby(['year','month','wld_id']).sum()
    
    return [ymb, ymbp, ymbw, ymp, ympw, ymw]