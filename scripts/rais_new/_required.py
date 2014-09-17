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

def required(ybio, ybi, yi, year):
    
    # print "reset index", ybio.index.is_unique
    ybio = ybio.reset_index()
    ybio_data = ybio[["bra_id","cnae_id","cbo_id","num_emp"]]
    cnae_criterion = ybio_data['cnae_id'].map(lambda x: len(x) == 5)
    cbo_criterion = ybio_data['cbo_id'].map(lambda x: len(str(x)) == 4)
    ybio_data = ybio_data[cnae_criterion & cbo_criterion]
    
    ybi = ybi.reset_index()
    cnae_criterion = ybi['cnae_id'].map(lambda x: len(x) == 5)
    ybi = ybi[cnae_criterion]
    ybi = ybi[["bra_id", "cnae_id", "num_emp_est"]]
    
    yi = yi.reset_index()
    cnae_criterion = yi['cnae_id'].map(lambda x: len(x) == 5)
    yi = yi[cnae_criterion]
    yi = yi[["cnae_id", "num_emp_est"]]
    yi = yi.set_index("cnae_id")["num_emp_est"]
    
    ybio_required = []
    for geo_level in [2, 8]:
        bra_criterion = ybio_data['bra_id'].map(lambda x: len(x) == geo_level)
        ybio_panel = ybio_data[bra_criterion]
        ybio_panel = ybio_panel.pivot_table(index=["bra_id", "cbo_id"], \
                                            columns="cnae_id", \
                                            values="num_emp")
        ybio_panel = ybio_panel.to_panel()
        
        bra_criterion = ybi['bra_id'].map(lambda x: len(x) == geo_level)
        ybi_ras = ybi[bra_criterion]
        # print ybi_ras.head()
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
    
    
    # print "merging datasets..."
    # ybio_required = pd.DataFrame(ybio_required, columns=["year", "bra_id", "isic_id", "cbo_id", "required"])
    # ybio_required = ybio_required.set_index(["year", "bra_id", "isic_id", "cbo_id"])
    # 
    # ybio = ybio.set_index(["year", "bra_id", "isic_id", "cbo_id"])
    # ybio["required"] = ybio_required["required"]
    
    print "merging datasets..."
    ybio_required = pd.DataFrame(ybio_required, columns=["year", "bra_id", "cnae_id", "cbo_id", "required"])
    ybio_required['year'] = ybio_required['year'].astype(int)
    ybio['year'] = ybio['year'].astype(int)
    ybio = pd.merge(ybio, ybio_required, on=["year", "bra_id", "cnae_id", "cbo_id"], how="outer").fillna(0)
    
    ybio = ybio.set_index(["year", "bra_id", "cnae_id", "cbo_id"])
    
    return ybio
    
