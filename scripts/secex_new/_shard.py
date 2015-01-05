def shard(ybpw, depths):
    ybpw = ybpw.reset_index()
    
    bra_criterion = ybpw.bra_id.map(lambda x: len(x) == depths["bra"][-1])
    hs_criterion = ybpw.hs_id.map(lambda x: len(x) == depths["hs"][-1])
    wld_criterion = ybpw.wld_id.map(lambda x: len(x) == depths["wld"][-1])
    
    yb = ybpw[wld_criterion & hs_criterion]
    yb = yb.groupby(['year','bra_id']).sum()
    
    ybp = ybpw[wld_criterion]
    ybp = ybp.groupby(['year','bra_id','hs_id']).sum()
    
    ybw = ybpw[hs_criterion]
    ybw = ybw.groupby(['year','bra_id','wld_id']).sum()
    
    yp = ybpw[bra_criterion & wld_criterion]
    yp = yp.groupby(['year','hs_id']).sum()
    
    ypw = ybpw[bra_criterion]
    ypw = ypw.groupby(['year','hs_id','wld_id']).sum()
    
    yw = ybpw[bra_criterion & hs_criterion]
    yw = yw.groupby(['year','wld_id']).sum()
    
    return [yb, ybp, ybw, yp, ypw, yw]