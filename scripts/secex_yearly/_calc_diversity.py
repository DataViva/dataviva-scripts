import sys
import pandas as pd
import numpy as np

def get_deepest(column, depths):
    if column == "hs_id": return depths["hs"][-1]
    if column == "bra_id": return depths["bra"][-1]
    if column == "wld_id": return depths["wld"][-1]

def calc_diversity(diversity_tbl, return_tbl, index_col, diversity_col, depths):
    
    # filter table by deepest length
    diversity_tbl = diversity_tbl.reset_index()
    year = diversity_tbl['year'][0]
    deepest_criterion = diversity_tbl[diversity_col].map(lambda x: len(x) == get_deepest(diversity_col, depths))
    diversity_tbl = diversity_tbl[deepest_criterion]
    
    '''
        GET DIVERSITY
    '''
    diversity = diversity_tbl.pivot(index=index_col, columns=diversity_col, values="val_usd").fillna(0)
    diversity[diversity >= 1] = 1
    diversity[diversity < 1] = 0
    diversity = diversity.sum(axis=1)
    
    '''
        GET EFFECTIVE DIVERSITY
    '''
    entropy = diversity_tbl.pivot(index=index_col, columns=diversity_col, values="val_usd").fillna(0)
    entropy = entropy.T / entropy.T.sum()
    entropy = entropy * np.log(entropy)
    
    entropy = entropy.sum() * -1
    es = pd.Series([np.e]*len(entropy), index=entropy.index)
    effective_diversity = es ** entropy

    '''
        ADD DIVERSITY TO RETURN TABLE
    '''
    prefix = diversity_col.replace("_id", "")
    for tbl_col in [(diversity, "{0}_diversity".format(prefix)), 
                    (effective_diversity, "{0}_diversity_eff".format(prefix))]:
        tbl, col = tbl_col
        tbl = pd.DataFrame(tbl, columns=[col])
        tbl["year"] = year
        tbl = tbl.reset_index()
        tbl = tbl.set_index(["year", index_col])
        return_tbl[col] = tbl[col]

    return return_tbl
