import sys, os
import pandas as pd
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../../../lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

def calc_complexity(ypw):
    
    ubiquity_required = 20
    diversity_required = 200
    total_exports_required = 50000000
    
    '''trim country list by diversity'''
    origin_diversity = ypw.reset_index()
    origin_diversity = origin_diversity["wld_id"].value_counts()
    origin_diversity = origin_diversity[origin_diversity > diversity_required]
    
    '''trim country list by total exports'''
    origin_totals = ypw.groupby(level=['wld_id']).sum()
    origin_totals = origin_totals['val_usd']
    origin_totals = origin_totals[origin_totals > total_exports_required]
    
    filtered_origins = set(origin_diversity.index).intersection(set(origin_totals.index))
    
    '''trim product list by ubiquity'''
    product_ubiquity = ypw.reset_index()
    product_ubiquity = product_ubiquity["hs_id"].value_counts()
    product_ubiquity = product_ubiquity[product_ubiquity > ubiquity_required]
    
    filtered_products = set(product_ubiquity.index)
    
    '''re-calculate rcas'''
    origins_to_drop = set(ypw.index.get_level_values('wld_id')).difference(filtered_origins)
    products_to_drop = set(ypw.index.get_level_values('hs_id')).difference(filtered_products)
    
    ypw = ypw.drop(list(origins_to_drop), axis=0, level='wld_id')
    ypw = ypw.drop(list(products_to_drop), axis=0, level='hs_id')
    ypw_rca = ypw.reset_index()
    ypw_rca = ypw_rca.pivot(index="wld_id", columns="hs_id", values="val_usd")
    ypw_rca = ps_calcs.rca(ypw_rca)
    
    ypw_rca[ypw_rca >= 1] = 1
    ypw_rca[ypw_rca < 1] = 0
    
    return ps_calcs.complexity(ypw_rca)
