import sys, os
import pandas as pd

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../", "lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

def get_ybp_rcas(ybp, geo_level, depths):
    
    ybp = ybp.reset_index()
    hs_criterion = ybp['hs_id'].map(lambda x: len(x) == depths["hs"][-1])
    bra_criterion = ybp['bra_id'].map(lambda x: len(x) == geo_level)
    
    ybp = ybp[hs_criterion & bra_criterion]
    ybp = ybp[["bra_id","hs_id","val_usd"]]
    
    ybp = ybp.pivot(index="bra_id", columns="hs_id", values="val_usd").fillna(0)
    
    rcas = ps_calcs.rca(ybp)
    rcas[rcas >= 1] = 1
    rcas[rcas < 1] = 0
    
    return rcas

def domestic_eci(yp, yb, ybp, depths):
    yp = yp.reset_index()
    year = yp['year'][0]
    
    hs_criterion = yp['hs_id'].map(lambda x: len(x) == depths["hs"][-1])
    
    yp = yp[hs_criterion & pd.notnull(yp['pci'])]
    yp = yp[["hs_id", "pci"]]
    yp = yp.set_index("hs_id")
    
    pcis = yp.T
    
    ecis = []
    for geo_level in depths["bra"]:
        print "geo_level:",geo_level

        rcas = get_ybp_rcas(ybp, geo_level, depths)
        
        rcas = rcas.reindex(columns=pcis.columns)

        geo_level_pcis = pd.DataFrame([pcis.values[0]]*len(rcas.index), columns=pcis.columns, index=rcas.index)

        geo_level_ecis = rcas * geo_level_pcis
        geo_level_ecis = geo_level_ecis.sum(axis=1)
        geo_level_ecis = geo_level_ecis / rcas.sum(axis=1)

        ecis.append(geo_level_ecis)

    ecis = pd.concat(ecis)
    ecis = pd.DataFrame(ecis, columns=["eci"])
    ecis["year"] = year
    
    ecis = ecis.reset_index()
    ecis = ecis.set_index(["year", "bra_id"])
    
    yb["eci"] = ecis["eci"]
    
    
    return yb
