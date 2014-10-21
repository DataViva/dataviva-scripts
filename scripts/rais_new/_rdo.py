import sys, os
import pandas as pd
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
growth_lib_path = os.path.abspath(os.path.join(file_path, "..", "growth_lib"))
sys.path.insert(0, growth_lib_path)
import growth

def rdo(ybi, yi, year, geo_depths):
    
    rca_dist_opp = []
    for geo_level in geo_depths:
        print "geo level:", geo_level
        
        ybi_data = ybi.reset_index()
        
        bra_criterion = ybi_data["bra_id"].map(lambda x: len(x) == geo_level)
        cnae_criterion = ybi_data["cnae_id"].map(lambda x: len(x) == 6)
        ybi_data = ybi_data[bra_criterion & cnae_criterion]
        
        # ybi_data = ybi_data.reindex(index=ybi_index)
        # ybi_data = ybi_data.drop(["year", "num_emp", "num_est", "wage_avg", "num_emp_est"], axis=1)
        ybi_data = ybi_data[["bra_id", "cnae_id", "wage"]]
    
        # ybi_data = ybi_data.unstack()
        # levels = ybi_data.columns.levels
        # labels = ybi_data.columns.labels
        # ybi_data.columns = levels[1][labels[1]]

        '''
            RCAS
        '''
        
        # ybi_data = ybi_data.pivot(index="bra_id", columns="cnae_id", values="wage").fillna(0)
        ybi_data = ybi_data.pivot(index="bra_id", columns="cnae_id", values="wage")
        rcas = growth.rca(ybi_data)
    
        rcas_binary = rcas.copy()
        rcas_binary[rcas_binary >= 1] = 1
        rcas_binary[rcas_binary < 1] = 0
    
        '''
            DISTANCES
        '''
    
        '''calculate proximity for opportunity gain calculation'''    
        prox = growth.proximity(rcas_binary)
        '''calculate distances using proximity'''    
        dist = growth.distance(rcas_binary, prox).fillna(0)
    
        '''
            OPP GAIN
        '''
    
        '''calculate product complexity'''
        pci = growth.complexity(rcas_binary)[1]
        '''calculate opportunity gain'''
        opp_gain = growth.opportunity_gain(rcas_binary, prox, pci)
    
        rdo = []
        for bra in rcas.index:
            for cnae in rcas.columns:
                rdo.append([year, bra, cnae, rcas[cnae][bra], dist[cnae][bra], opp_gain[cnae][bra]])
    
        rca_dist_opp += rdo
    
    # now time to merge!
    print "merging datasets..."
    ybi_rdo = pd.DataFrame(rca_dist_opp, columns=["year", "bra_id", "cnae_id", "rca", "distance", "opp_gain"])
    ybi_rdo["year"] = ybi_rdo["year"].astype(int)
    ybi_rdo["rca"][ybi_rdo["rca"] == 0] = np.nan
    ybi_rdo = ybi_rdo.set_index(["year", "bra_id", "cnae_id"])
    
    # get union of both sets of indexes
    all_ybi_indexes = set(ybi.index).union(set(ybi_rdo.index))
    
    all_ybi_indexes = pd.MultiIndex.from_tuples(all_ybi_indexes, names=["year", "bra_id", "cnae_id"])
    # ybi = ybi.reindex(index=all_ybi_indexes, fill_value=0)
    ybi = ybi.reindex(index=all_ybi_indexes)
    ybi["rca"] = ybi_rdo["rca"]
    ybi["distance"] = ybi_rdo["distance"]
    ybi["opp_gain"] = ybi_rdo["opp_gain"]
    
    return ybi