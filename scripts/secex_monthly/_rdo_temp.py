import MySQLdb, sys, os
import pandas as pd
import pandas.io.sql as sql
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../", "lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

def get_wld_rcas(geo_level, year, ymbp):
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
    ymbp = ymbp.reset_index()
    
    month_criterion = ymbp['month'].map(lambda x: x == "00")
    hs_criterion = ymbp['hs_id'].map(lambda x: len(x) == 6)
    bra_criterion = ymbp['bra_id'].map(lambda x: len(x) == geo_level)
    
    ymbp = ymbp[month_criterion & hs_criterion & bra_criterion]
    ymbp = ymbp[["bra_id", "hs_id", "export_val"]]
    ymbp = ymbp.pivot(index="bra_id", columns="hs_id", values="export_val").fillna(0)
    
    '''Get world values from database'''
    q = "select wld_id, hs_id, val_usd from comtrade_ypw where year = {0}".format(year)
    ybp_wld = sql.read_sql(q, db)
    ybp_wld = ybp_wld.rename(columns={"val_usd":"export_val"})
    ybp_wld = ybp_wld.pivot(index="wld_id", columns="hs_id", values="export_val")
    ybp_wld = ybp_wld.reindex(columns=ymbp.columns)
    ybp_wld = ybp_wld.fillna(0)
    
    mcp = rca(ymbp, ybp_wld).fillna(0)
    mcp[mcp == np.inf] = 0
    
    return mcp

def get_domestic_rcas(geo_level, year, ymbp, trade_flow):
    ymbp = ymbp.reset_index()
    val_col = trade_flow+"_val"
    
    month_criterion = ymbp['month'].map(lambda x: x == "00")
    hs_criterion = ymbp['hs_id'].map(lambda x: len(x) == 6)
    bra_criterion = ymbp['bra_id'].map(lambda x: len(x) == geo_level)
    
    ymbp = ymbp[month_criterion & hs_criterion & bra_criterion]
    ymbp = ymbp[["bra_id", "hs_id", val_col]]
    ymbp = ymbp.pivot(index="bra_id", columns="hs_id", values=val_col).fillna(0)
    
    rcas = ps_calcs.rca(ymbp).fillna(0)
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
    table = table.rename(columns={"val_usd":"export_val"})
    table = table.pivot(index="wld_id", columns="hs_id", values="export_val")
    table = table.fillna(0)

    '''Use growth library to run RCA calculation on data'''
    mcp = ps_calcs.rca(table)
    mcp[mcp >= 1] = 1
    mcp[mcp < 1] = 0
    
    prox = ps_calcs.proximity(mcp)

    return prox

def get_pcis(geo_level, ymp):
    
    ymp = ymp.reset_index()
    month_criterion = ymp['month'].map(lambda x: x == "00")
    hs_criterion = ymp['hs_id'].map(lambda x: len(x) == 6)
    
    ymp = ymp[month_criterion & hs_criterion]
    # ymp = ymp.drop(["year", "val_usd"], axis=1)
    ymp = ymp[["hs_id", "pci"]]
    
    ymp = ymp.set_index("hs_id")
    ymp = ymp[pd.notnull(ymp.pci)]
    
    return ymp["pci"]

def rdo(ymbp, ymp, year, geo_depths):
    
    export_hs = ymp[["export_val"]].groupby(level=["hs_id"]).sum().dropna()
    export_hs = [hs for hs in export_hs.index if len(hs) == 6]
    
    import_hs = ymp[["import_val"]].groupby(level=["hs_id"]).sum().dropna()
    import_hs = [hs for hs in import_hs.index if len(hs) == 6]
    
    rca_dist_opp = []
    for geo_level in geo_depths:
        print "geo_level",geo_level

        '''
            RCAS
        '''
        rcas_dom = get_domestic_rcas(geo_level, year, ymbp, "export")
        rcas_dom = rcas_dom.reindex(columns=export_hs)
        
        rcd = get_domestic_rcas(geo_level, year, ymbp, "import")
        rcd = rcd.reindex(columns=import_hs)
        # print rcd.ix["mg"]
        # sys.exit()
        
        rcas_wld = get_wld_rcas(geo_level, year, ymbp)
        rcas_wld = rcas_wld.reindex(columns=export_hs)
        # print rcas_wld.ix["4"]
        # print rcas_wld['010204']
        # sys.exit()
    
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
        prox_dom = ps_calcs.proximity(rcas_dom_binary)
        dist_dom = ps_calcs.distance(rcas_dom_binary, prox_dom).fillna(0)
        
        '''world distances'''
        prox_wld = get_wld_proximity(year)
        hs_wld = set(rcas_wld_binary.columns).intersection(set(prox_wld.columns))

        # hs_wld = set(rcas_wld_binary.columns).union(set(prox_wld.columns))
        prox_wld = prox_wld.reindex(columns=hs_wld, index=hs_wld)
        rcas_wld_binary = rcas_wld_binary.reindex(columns=hs_wld)
        
        dist_wld = ps_calcs.distance(rcas_wld_binary, prox_wld).fillna(0)
        
        '''
            OPP GAIN
        '''

        '''same PCIs for all since we are using world PCIs'''
        pcis = get_pcis(geo_level, ymp)

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
        opp_gain_wld = ps_calcs.opportunity_gain(rcas_wld_binary, prox_wld, pcis_wld)
        opp_gain_dom = ps_calcs.opportunity_gain(rcas_dom_binary, prox_dom, pcis_wld)
        
        '''
            SET RCAS TO NULL
        '''
        rcas_dom = rcas_dom.replace(0, np.nan)
        rcas_wld = rcas_wld.replace(0, np.nan)
        rcd = rcd.replace(0, np.nan)
        
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
        
        ''' Connect to DB '''
        db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), 
                             user=os.environ["DATAVIVA_DB_USER"], 
                             passwd=os.environ["DATAVIVA_DB_PW"], 
                             db=os.environ["DATAVIVA_DB_NAME"])
        db.autocommit(1)
        cursor = db.cursor()
        
        for bra in set(rcas_dom.index).union(set(rcas_wld.index)):
            for hs in set(export_hs).union(set(import_hs)):
                cursor.execute("update secex_ymbp set rca_wld=%s, opp_gain_wld=%s, distance_wld=%s where year=%s and month=0 and bra_id=%s and hs_id=%s;",[tryto(rcas_wld, hs, bra), tryto(opp_gain_wld, hs, bra), tryto(dist_wld, hs, bra), year, bra, hs])
                # rca_dist_opp.append([year, bra, hs, \
                #                 tryto(rcas_dom, hs, bra), tryto(rcas_wld, hs, bra), \
                #                 tryto(rcd, hs, bra), \
                #                 tryto(dist_dom, hs, bra), tryto(dist_wld, hs, bra), \
                #                 tryto(opp_gain_dom, hs, bra), tryto(opp_gain_wld, hs, bra) ])
        
        # print len(rca_dist_opp), "rows updated"
        
    # now time to merge!
    # print "merging datasets..."
    
    # ybp_rdo = pd.DataFrame(rca_dist_opp, columns=["year", "bra_id", "hs_id", "rca", "rca_wld", "rcd", "distance", "distance_wld", "opp_gain", "opp_gain_wld"])
    # ybp_rdo["year"] = ybp_rdo["year"].astype("int")
    # ybp_rdo["month"] = "00"
    # ybp_rdo = ybp_rdo.set_index(["year", "month", "bra_id", "hs_id"])
    #
    # ymbp = pd.merge(ymbp, ybp_rdo, how="outer", left_index=True, right_index=True)
    #
    # return ymbp