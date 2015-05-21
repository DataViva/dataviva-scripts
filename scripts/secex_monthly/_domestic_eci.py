import sys, os, MySQLdb
import pandas as pd

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../", "lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), 
                     user=os.environ["DATAVIVA_DB_USER"], 
                     passwd=os.environ["DATAVIVA_DB_PW"], 
                     db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def get_ybp_rcas(ymbp, geo_level):
    
    ymbp = ymbp.reset_index()
    month_criterion = ymbp['month'].map(lambda x: x == '00')
    hs_criterion = ymbp['hs_id'].map(lambda x: len(x) == 6)
    bra_criterion = ymbp['bra_id'].map(lambda x: len(x) == geo_level)
    ymbp = ymbp[month_criterion & hs_criterion & bra_criterion]
    ymbp = ymbp[["bra_id","hs_id","export_val"]]
    ymbp = ymbp.pivot(index="bra_id", columns="hs_id", values="export_val").fillna(0)
    
    # ymb = ymb.reset_index()
    # month_criterion = ymb['month'].map(lambda x: x == '00')
    # bra_criterion = ymb['bra_id'].map(lambda x: len(x) == geo_level)
    # ymb = ymb[month_criterion & bra_criterion]
    # # threshold... bras w/ product diversity > 1
    # allowed_bras = ymb[ymb.hs_diversity > 1].set_index("bra_id").index
    # ymbp = ymbp.reindex(allowed_bras)
    
    rcas = ps_calcs.rca(ymbp)
    rcas[rcas >= 1] = 1
    rcas[rcas < 1] = 0
    rcas = rcas.fillna(0)
    
    prod_diversity = rcas.sum(axis=1)
    allowed_bras = prod_diversity[prod_diversity>0].index
    
    rcas = rcas.reindex(allowed_bras)
    ymbp = ymbp.reindex(allowed_bras)
    
    shares = ymbp * rcas
    
    return shares

def domestic_eci(ymp, ymb, ymbp, geo_depths):
    ymp = ymp.reset_index()
    year = ymp['year'][0]
    
    hs_criterion = ymp['hs_id'].map(lambda x: len(x) == 6)
    
    ymp = ymp[hs_criterion & pd.notnull(ymp['pci'])]
    ymp = ymp[["hs_id", "pci"]]
    ymp = ymp.set_index("hs_id")
    
    pcis = ymp.T
    
    ecis = []
    # for geo_level in [9]:
    for geo_level in geo_depths:
        print "geo_level:",geo_level

        shares = get_ybp_rcas(ymbp, geo_level)
        
        shares = shares.reindex(columns=pcis.columns)
        shares = (shares.T / shares.sum(axis=1)).T
        
        # allowed_bras = shares.sum(axis=1).dropna().index
        # shares = shares.reindex(allowed_bras)

        geo_level_pcis = pd.DataFrame([pcis.values[0]]*len(shares.index), columns=pcis.columns, index=shares.index)

        geo_level_ecis = shares * geo_level_pcis
        # geo_level_ecis = geo_level_ecis.mean(axis=1)
        geo_level_ecis = pd.DataFrame({"eci":geo_level_ecis.sum(axis=1)})
        
        allowed_bras = ymb.copy().reset_index()
        month_criterion = allowed_bras['month'].map(lambda x: x == '00')
        bra_criterion = allowed_bras['bra_id'].map(lambda x: len(x) == geo_level)
        diversity_crit = allowed_bras.hs_diversity > 5
        export_val_crit = allowed_bras.export_val > ymb.export_val.quantile(.5)
        allowed_bras = allowed_bras[month_criterion & bra_criterion & export_val_crit & diversity_crit]
        allowed_bras = allowed_bras.set_index("bra_id").index
        
        geo_level_ecis = geo_level_ecis.reindex(allowed_bras)
        
        # print geo_level_ecis.eci.order()
        
        # for bra_id, eci in geo_level_ecis.eci.iteritems():
        #     if eci:
        #         cursor.execute("update secex_ymb set eci = %s where year=%s and month=0 and bra_id=%s;", [eci, year, bra_id])

        ecis.append(geo_level_ecis)

    ecis = pd.concat(ecis)
    ecis = pd.DataFrame(ecis, columns=["eci"])
    ecis["year"] = year
    ecis["month"] = "00"
    
    ecis = ecis.reset_index()
    ecis = ecis.set_index(["year", "month", "bra_id"])
    
    ymb["eci"] = ecis["eci"]
    
    
    return ymb
