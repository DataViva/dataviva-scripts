import MySQLdb, sys, os
import pandas as pd
import pandas.io.sql as sql
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
growth_lib_path = os.path.abspath(os.path.join(file_path, "..", "growth_lib"))
sys.path.insert(0, growth_lib_path)
import growth

def get_wld_rcas(geo_level, year, ybp, depths):
    ''' Connect to DB '''
    db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    
    def rca(bra_tbl, wld_tbl):
        col_sums = bra_tbl.sum(axis=1)
        col_sums = col_sums.reshape((len(col_sums), 1))
        rca_numerator = np.divide(bra_tbl, col_sums)
    
        row_sums = wld_tbl.sum(axis=0)
        total_sum = wld_tbl.sum().sum()
        rca_denominator = row_sums / total_sum
        rcas = rca_numerator / rca_denominator
        return rcas

    '''Get domestic values from TSV'''
    ybp = ybp.reset_index()
    
    hs_criterion = ybp['hs_id'].map(lambda x: len(x) == depths['hs'][-1])
    bra_criterion = ybp['bra_id'].map(lambda x: len(x) == geo_level)
    
    ybp = ybp[hs_criterion & bra_criterion]
    ybp = ybp[["bra_id", "hs_id", "val_usd"]]
    ybp = ybp.pivot(index="bra_id", columns="hs_id", values="val_usd").fillna(0)
    
    '''Get world values from database'''
    q = "select wld_id, hs_id, val_usd from comtrade_ypw where year = {0}".format(year)
    ybp_wld = sql.read_sql(q, db)
    # ybp_wld = ybp_wld.rename(columns={"val_usd":"val_usd"})
    ybp_wld = ybp_wld.pivot(index="wld_id", columns="hs_id", values="val_usd")
    ybp_wld = ybp_wld.reindex(columns=ybp.columns)
    ybp_wld = ybp_wld.fillna(0)
    
    mcp = rca(ybp, ybp_wld).fillna(0)
    mcp[mcp == np.inf] = 0
    
    return mcp

def get_domestic_rcas(geo_level, year, ybp, depths):
    ybp = ybp.reset_index()
    
    hs_criterion = ybp['hs_id'].map(lambda x: len(x) == depths['hs'][-1])
    bra_criterion = ybp['bra_id'].map(lambda x: len(x) == geo_level)
    
    ybp = ybp[hs_criterion & bra_criterion]
    ybp = ybp[["bra_id", "hs_id", "val_usd"]]
    ybp = ybp.pivot(index="bra_id", columns="hs_id", values="val_usd").fillna(0)
    
    rcas = growth.rca(ybp).fillna(0)
    rcas[rcas == np.inf] = 0
    
    return rcas

def get_wld_proximity(year):
    ''' Connect to DB '''
    db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])

    '''Get values from database'''
    q = "select wld_id, hs_id, val_usd " \
        "from comtrade_ypw " \
        "where year = {0} and length(hs_id) = 6".format(year)
    table = sql.read_sql(q, db)
    table = table.rename(columns={"val_usd":"val_usd"})
    table = table.pivot(index="wld_id", columns="hs_id", values="val_usd")
    table = table.fillna(0)

    '''Use growth library to run RCA calculation on data'''
    mcp = growth.rca(table)
    mcp[mcp >= 1] = 1
    mcp[mcp < 1] = 0
    
    prox = growth.proximity(mcp)

    return prox

def get_pcis(geo_level, yp, depths):
    
    yp = yp.reset_index()
    hs_criterion = yp['hs_id'].map(lambda x: len(x) == depths['hs'][-1])
    
    yp = yp[hs_criterion]
    # ymp = ymp.drop(["year", "val_usd"], axis=1)
    yp = yp[["hs_id", "pci"]]
    
    yp = yp.set_index("hs_id")
    yp = yp[pd.notnull(yp.pci)]
    
    return yp["pci"]

def rdo(ybp, yp, year, depths):
    
    hs = yp[["val_usd"]].groupby(level=["hs_id"]).sum().dropna()
    hs = [h for h in hs.index if len(h) == depths["hs"][-1]]
    
    rca_dist_opp = []
    for geo_level in depths["bra"]:
        print "geo_level",geo_level

        '''
            RCAS
        '''
        rcas_dom = get_domestic_rcas(geo_level, year, ybp, depths)
        rcas_dom = rcas_dom.reindex(columns=hs)
        
        rcas_wld = get_wld_rcas(geo_level, year, ybp, depths)
        rcas_wld = rcas_wld.reindex(columns=hs)
    
        rcas_dom_binary = rcas_dom.copy()
        rcas_dom_binary[rcas_dom_binary >= 1] = 1
        rcas_dom_binary[rcas_dom_binary < 1] = 0
        
        rcas_wld_binary = rcas_wld.copy()
        rcas_wld_binary[rcas_wld_binary >= 1] = 1
        rcas_wld_binary[rcas_wld_binary < 1] = 0
        
        '''
            DISTANCES
        '''
        '''domestic distances'''
        prox_dom = growth.proximity(rcas_dom_binary)
        dist_dom = growth.distance(rcas_dom_binary, prox_dom).fillna(0)
        
        '''world distances'''
        prox_wld = get_wld_proximity(year)
        hs_wld = set(rcas_wld_binary.columns).intersection(set(prox_wld.columns))

        # hs_wld = set(rcas_wld_binary.columns).union(set(prox_wld.columns))
        prox_wld = prox_wld.reindex(columns=hs_wld, index=hs_wld)
        rcas_wld_binary = rcas_wld_binary.reindex(columns=hs_wld)
        
        dist_wld = growth.distance(rcas_wld_binary, prox_wld).fillna(0)
        
        '''
            OPP GAIN
        '''

        '''same PCIs for all since we are using world PCIs'''
        pcis = get_pcis(geo_level, yp, depths)

        # all_hs_dom = set(pcis.index).union(set(rcas_dom.columns))
        all_hs_dom = set(pcis.index).intersection(set(rcas_dom.columns))
        pcis_dom = pcis.reindex(index=all_hs_dom)
        rcas_dom_binary = rcas_dom_binary.reindex(columns = all_hs_dom)
        prox_dom = prox_dom.reindex(index = all_hs_dom, columns = all_hs_dom)

        # print rcas_dom_binary.shape, prox_dom.shape, pcis.shape

        # all_hs_wld = set(pcis.index).union(set(rcas_wld.columns))
        all_hs_wld = set(pcis.index).intersection(set(rcas_wld.columns))
        pcis_wld = pcis.reindex(index=all_hs_wld)
        rcas_wld_binary = rcas_wld_binary.reindex(columns = all_hs_wld)
        prox_wld = prox_wld.reindex(index = all_hs_wld, columns = all_hs_wld)

        # print rcas_dom_binary.shape, prox_dom.shape, pcis.shape
        opp_gain_wld = growth.opportunity_gain(rcas_wld_binary, prox_wld, pcis_wld)
        opp_gain_dom = growth.opportunity_gain(rcas_dom_binary, prox_dom, pcis_wld)
        
        '''
            SET RCAS TO NULL
        '''
        rcas_dom = rcas_dom.replace(0, np.nan)
        rcas_wld = rcas_wld.replace(0, np.nan)
        
        def tryto(df, col, ind):
            if col in df.columns:
                if ind in df.index:
                    return df[col][ind]
            return None
        
        # print opp_gain_wld.ix["al000107"].ix["041601"]
        # print opp_gain_dom.ix["al000107"].ix["041601"]
        # print tryto(opp_gain_dom, "041601", "al000107")
        # print "al000107" in set(rcas_dom.index).union(set(rcas_wld.index))
        # print "041601" in set(export_hs).union(set(import_hs))
        # sys.exit()
        
        for bra in set(rcas_dom.index).union(set(rcas_wld.index)):
            for h in hs:
                rca_dist_opp.append([year, bra, h, \
                                tryto(rcas_dom, h, bra), tryto(rcas_wld, h, bra), \
                                tryto(dist_dom, h, bra), tryto(dist_wld, h, bra), \
                                tryto(opp_gain_dom, h, bra), tryto(opp_gain_wld, h, bra) ])
        
        # print len(rca_dist_opp), "rows updated"
        
    # now time to merge!
    # print "merging datasets..."
    ybp_rdo = pd.DataFrame(rca_dist_opp, columns=["year", "bra_id", "hs_id", "rca", "rca_wld", "distance", "distance_wld", "opp_gain", "opp_gain_wld"])
    ybp_rdo["year"] = ybp_rdo["year"].astype("int")
    ybp_rdo = ybp_rdo.set_index(["year", "bra_id", "hs_id"])
    
    ybp = pd.merge(ybp, ybp_rdo, how="outer", left_index=True, right_index=True)
    
    return ybp