import sys, os
import pandas as pd

file_path = os.path.dirname(os.path.realpath(__file__))
growth_lib_path = os.path.abspath(os.path.join(file_path, "..", "growth_lib"))
sys.path.insert(0, growth_lib_path)
import growth

def get_ybi_rcas(ybi, geo_level):
    ybi = ybi.reset_index()
    
    cnae_criterion = ybi['cnae_id'].str.len() == 6
    bra_criterion = ybi['bra_id'].str.len() == geo_level
    
    ybi = ybi[cnae_criterion & bra_criterion]
    ybi = ybi[["bra_id", "cnae_id", "wage"]]
    
    ybi = ybi.pivot(index="bra_id", columns="cnae_id", values="wage").fillna(0)
    
    rcas = growth.rca(ybi)
    rcas[rcas >= 1] = 1
    rcas[rcas < 1] = 0
    
    return rcas

def ybio_to_panel(ybio, geo_depths):
    ybio = ybio.reset_index()
    cnae_criterion = ybio['cnae_id'].map(lambda x: len(x) == 6)
    cbo_criterion = ybio['cbo_id'].map(lambda x: len(x) == 4)
    bra_criterion = ybio['bra_id'].map(lambda x: len(x) == geo_depths[-1])
    ybio = ybio[cnae_criterion & cbo_criterion & bra_criterion]
    
    ybio = ybio[["cnae_id", "cbo_id", "bra_id", "num_jobs"]]
    print "pivoting YBIO..."
    ybio = ybio.pivot_table(index=["cnae_id", "cbo_id"], columns="bra_id", values="num_jobs")
    ybio = ybio.fillna(0)
    
    panel = ybio.to_panel()
    panel = panel.swapaxes("items", "minor")
    panel = panel.swapaxes("major", "minor")
    
    return panel

def importance(ybio, ybi, yio, yo, year, geo_depths):
    year = int(year)
    yo = yo.reset_index(level="year")
    all_cbo = [cbo for cbo in list(yo.index) if len(cbo) == 4]
    
    '''get ybi RCAs'''
    rcas = get_ybi_rcas(ybi, geo_depths[-1])
    
    denoms = rcas.sum()
    
    # z       = occupations
    # rows    = bras
    # colums  = cnaes
    ybio = ybio_to_panel(ybio, geo_depths)
    
    yio_importance = []
    for cbo in all_cbo:
        
        try:
            num_jobs = ybio[cbo].fillna(0)
        except:
            continue
        numerators = num_jobs * rcas
        numerators = numerators.fillna(0)
        
        '''convert nominal num_jobs values to 0s and 1s'''
        numerators[numerators >= 1] = 1
        numerators[numerators < 1] = 0
        
        numerators = numerators.sum()
        importance = numerators / denoms
        # print importance[importance > 0]
        
        # print importance.index
        # sys.exit()
        
        for cnae in importance.index:
            imp = importance[cnae]
            yio_importance.append([year, cnae, cbo, imp])
        
        # print year, cbo, time.time() - start
        # sys.stdout.write('\r ' + year + ' CBO id: ' + cbo + ' '*10)
        print year, cbo
        # sys.stdout.flush() # important
    
    # now time to merge!
    print
    print "merging datasets..."
    yio_importance = pd.DataFrame(yio_importance, columns=["year", "cnae_id", "cbo_id", "importance"])
    yio_importance = yio_importance.set_index(["year", "cnae_id", "cbo_id"])
    
    yio = pd.merge(yio, yio_importance, how='left', left_index=True, right_index=True)
    # print yio.head()
    # yio.to_csv("imp_test.csv")
    
    return yio