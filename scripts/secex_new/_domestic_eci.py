import sys, os
import pandas as pd

file_path = os.path.dirname(os.path.realpath(__file__))
growth_lib_path = os.path.abspath(os.path.join(file_path, "..", "growth_lib"))
sys.path.insert(0, growth_lib_path)
import growth

def get_ybp_rcas(ymbp, geo_level):
    
    ymbp = ymbp.reset_index()
    month_criterion = ymbp['month'].map(lambda x: x == '00')
    hs_criterion = ymbp['hs_id'].map(lambda x: len(x) == 6)
    bra_criterion = ymbp['bra_id'].map(lambda x: len(x) == geo_level)
    
    ymbp = ymbp[month_criterion & hs_criterion & bra_criterion]
    ymbp = ymbp[["bra_id","hs_id","export_val"]]
    
    ymbp = ymbp.pivot(index="bra_id", columns="hs_id", values="export_val").fillna(0)
    
    rcas = growth.rca(ymbp)
    rcas[rcas >= 1] = 1
    rcas[rcas < 1] = 0
    
    return rcas

def domestic_eci(ymp, ymb, ymbp, geo_depths):
    ymp = ymp.reset_index()
    year = ymp['year'][0]
    
    hs_criterion = ymp['hs_id'].map(lambda x: len(x) == 6)
    
    ymp = ymp[hs_criterion & pd.notnull(ymp['pci'])]
    ymp = ymp[["hs_id", "pci"]]
    ymp = ymp.set_index("hs_id")
    
    pcis = ymp.T
    
    ecis = []
    for geo_level in geo_depths:
        print "geo_level:",geo_level

        rcas = get_ybp_rcas(ymbp, geo_level)
        
        rcas = rcas.reindex(columns=pcis.columns)

        geo_level_pcis = pd.DataFrame([pcis.values[0]]*len(rcas.index), columns=pcis.columns, index=rcas.index)

        geo_level_ecis = rcas * geo_level_pcis
        geo_level_ecis = geo_level_ecis.sum(axis=1)
        geo_level_ecis = geo_level_ecis / rcas.sum(axis=1)

        ecis.append(geo_level_ecis)

    ecis = pd.concat(ecis)
    ecis = pd.DataFrame(ecis, columns=["eci"])
    ecis["year"] = year
    ecis["month"] = "00"
    
    ecis = ecis.reset_index()
    ecis = ecis.set_index(["year", "month", "bra_id"])
    
    ymb["eci"] = ecis["eci"]
    
    
    return ymb
