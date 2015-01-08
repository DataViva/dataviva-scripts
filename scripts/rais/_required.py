# -*- coding: utf-8 -*-
"""
    Caculate required numbers YBIO
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the third step in adding a new year of RAIS data to the 
    database. The script will output 1 bzipped TSV files that can then be 
    used throughout the rest of the steps.
    
    The year will needs to be specified by the user, the script will then
    loop through each geographic location to calculation the "required"
    number of employees for this YEAR-LOCATION-INDUSTRY-OCCUPATION combo.
"""

''' Import statements '''
import csv, sys, os, math, time, bz2
import pandas as pd
import numpy as np
# from ..helpers import get_file, format_runtime
# from ..growth_lib import growth

def required(ybio, ybi, yi, year, depths):
    
    # print "reset index", ybio.index.is_unique
    ybio = ybio.reset_index()
    ybio_data = ybio[["bra_id","cnae_id","cbo_id","num_emp"]]
    cnae_criterion = ybio_data['cnae_id'].str.len() == depths["cnae"][-1]
    cbo_criterion = ybio_data['cbo_id'].str.len() == depths["cbo"][-1]
    ybio_data = ybio_data[cnae_criterion & cbo_criterion]
    
    ybi = ybi.reset_index()
    ybi = ybi[ybi['cnae_id'].str.len() == depths["cnae"][-1]]
    ybi["num_emp_est"] = ybi["num_emp"] / ybi["num_est"]
    
    ybi = ybi[["bra_id", "cnae_id", "num_emp_est"]]
    
    yi = yi.reset_index()
    yi = yi[yi['cnae_id'].str.len() == depths["cnae"][-1]]
    yi["num_emp_est"] = yi["num_emp"] / yi["num_est"]
    yi = yi[["cnae_id", "num_emp_est"]]
    yi = yi.set_index("cnae_id")["num_emp_est"]
    
    ybio_required = []
    for geo_level in depths["bra"]:
        bra_criterion = ybio_data['bra_id'].str.len() == geo_level
        ybio_panel = ybio_data[bra_criterion]
        ybio_panel = ybio_panel.pivot_table(index=["bra_id", "cbo_id"], \
                                            columns="cnae_id", \
                                            values="num_emp")
        ybio_panel = ybio_panel.to_panel()
        
        bra_criterion = ybi['bra_id'].str.len() == geo_level
        ybi_ras = ybi[bra_criterion]
        ybi_ras = ybi_ras.pivot(index="bra_id", columns="cnae_id", values="num_emp_est").fillna(0)
        ybi_ras = ybi_ras / yi
        
        bras = ybi_ras.index
        for bra in bras:
            sys.stdout.write('\r current location: ' + bra + ' ' * 10)
            sys.stdout.flush() # important
            
            half_std = ybi_ras.std() / 2
            ras_similar_df = ((ybi_ras - ybi_ras.ix[bra]) / ybi_ras.std()).abs()
            
            # ras_similar_df = ras_similar_df <= half_std
            
            cnaes = ybi_ras.columns
            for cnae in cnaes:
                # print isic
                ras_similar = ras_similar_df[cnae][ras_similar_df[cnae] <= half_std[cnae]]
                
                # max only use top 20% of all locations
                max_cutoff = len(bras)*.2
                max_cutoff = 50
                ras_similar = ras_similar.order(ascending=False).index[:max_cutoff]
                
                if not len(ras_similar):
                    continue
                
                required_cbos = ybio_panel[cnae].ix[list(ras_similar)].fillna(0).mean(axis=0)
                required_cbos = required_cbos[required_cbos >= 1]
                
                for cbo in required_cbos.index:
                    ybio_required.append([year, bra, cnae, cbo, required_cbos[cbo]])
    
        print
        print "total required rows added:", len(ybio_required)
        # print ybio_required[:10]
        # sys.exit()
        
    print "merging datasets..."
    ybio_required = pd.DataFrame(ybio_required, columns=["year", "bra_id", "cnae_id", "cbo_id", "required"])
    ybio_required['year'] = ybio_required['year'].astype(int)
    # ybio_required['required'][ybio_required['required'] == 0] = np.nan
    ybio['year'] = ybio['year'].astype(int)
    ybio = pd.merge(ybio, ybio_required, on=["year", "bra_id", "cnae_id", "cbo_id"], how="outer")#.fillna(0)
    
    ybio = ybio.set_index(["year", "bra_id", "cnae_id", "cbo_id"])
    
    # print ybio.head()
    # print ybio.xs([2002, '1ac', 'a01113', '3117'])
    # sys.exit()
    
    return ybio
    
