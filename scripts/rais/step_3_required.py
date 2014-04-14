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
import csv, sys, os, argparse, math, time, bz2
from collections import defaultdict
from os import environ
import pandas as pd
import numpy as np
from ..helpers import get_file, format_runtime
from ..config import DATA_DIR
from ..growth_lib import growth
from scripts import YEAR, DELETE_PREVIOUS_FILE

def get_ybio(year):
    print "loading YBIO..."
    file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybio.tsv'))
    file = get_file(file_path)
    ybio = pd.read_csv(file, sep="\t", converters={"year": int, "cbo_id":str})
    
    return ybio

def get_yi(year):
    yi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yi.tsv'))
    yi_file = get_file(yi_file_path)
    yi = pd.read_csv(yi_file, sep="\t")

    isic_criterion = yi['isic_id'].map(lambda x: len(x) == 5)
    yi = yi[isic_criterion]
    yi = yi.drop(["year", "wage", "num_emp", "num_est", "wage_avg"], axis=1)
    yi = yi.set_index("isic_id")["num_emp_est"]
    
    return yi

def get_ybi(year, val):
    ybi_file_names = ['ybi.tsv', 'ybi_rcas_dist_opp.tsv', 'ybi_rcas_dist_opp_growth.tsv']
    for ybi_file_name in ybi_file_names:
        ybi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, ybi_file_name))
        ybi_file = get_file(ybi_file_path)
        if ybi_file:
            break
    
    ybi = pd.read_csv(ybi_file, sep="\t")
    
    isic_criterion = ybi['isic_id'].map(lambda x: len(x) == 5)
    
    ybi = ybi[isic_criterion]
    ybi = ybi[["bra_id", "isic_id"]+[val]]
    
    return ybi
    
def main(year, delete_previous_file):
    
    ybio = get_ybio(year)
    
    ybio_data = ybio.drop(["year", "wage", "num_est", "wage_avg", "num_emp_est"], axis=1)
    isic_criterion = ybio_data['isic_id'].map(lambda x: len(x) == 5)
    cbo_criterion = ybio_data['cbo_id'].map(lambda x: len(str(x)) == 4)
    ybio_data = ybio_data[isic_criterion & cbo_criterion]
    
    ybi = get_ybi(year, "num_emp_est")
    
    ybio_required = []
    for geo_level in [2, 4, 7, 8]:
        
        bra_criterion = ybio_data['bra_id'].map(lambda x: len(x) == geo_level)
        ybio_panel = ybio_data[bra_criterion]
        ybio_panel = ybio_panel.pivot_table(rows=["bra_id", "cbo_id"], \
                                            cols="isic_id", \
                                            values="num_emp")
        ybio_panel = ybio_panel.to_panel()

        yi = get_yi(year)
        
        bra_criterion = ybi['bra_id'].map(lambda x: len(x) == geo_level)
        ybi_ras = ybi[bra_criterion]
        ybi_ras = ybi_ras.pivot(index="bra_id", columns="isic_id", values="num_emp_est").fillna(0)
        ybi_ras = ybi_ras / yi
        
        bras = ybi_ras.index
        for bra in bras:
            sys.stdout.write('\r current location: ' + bra + ' ' * 10)
            sys.stdout.flush() # important
            
            half_std = ybi_ras.std() / 2
            ras_similar_df = ((ybi_ras - ybi_ras.ix[bra]) / ybi_ras.std()).abs()
            
            # ras_similar_df = ras_similar_df <= half_std
            
            isics = ybi_ras.columns
            for isic in isics:
                # print isic
                ras_similar = ras_similar_df[isic][ras_similar_df[isic] <= half_std[isic]]
                
                # max only use top 20% of all locations
                max_cutoff = len(bras)*.2
                max_cutoff = 50
                ras_similar = ras_similar.order(ascending=False).index[:max_cutoff]
                
                if not len(ras_similar):
                    continue
                
                required_cbos = ybio_panel[isic].ix[list(ras_similar)].fillna(0).mean(axis=0)
                required_cbos = required_cbos[required_cbos >= 1]
                
                for cbo in required_cbos.index:
                    ybio_required.append([year, bra, isic, cbo, required_cbos[cbo]])
    
        print
        print "total required rows added:", len(ybio_required)
    
    
    # print "merging datasets..."
    # ybio_required = pd.DataFrame(ybio_required, columns=["year", "bra_id", "isic_id", "cbo_id", "required"])
    # ybio_required = ybio_required.set_index(["year", "bra_id", "isic_id", "cbo_id"])
    # 
    # ybio = ybio.set_index(["year", "bra_id", "isic_id", "cbo_id"])
    # ybio["required"] = ybio_required["required"]
    
    print "merging datasets..."
    ybio_required = pd.DataFrame(ybio_required, columns=["year", "bra_id", "isic_id", "cbo_id", "required"])
    ybio_required['year'] = ybio_required['year'].astype(int)
    ybio['year'] = ybio['year'].astype(int)
    ybio = pd.merge(ybio, ybio_required, on=["year", "bra_id", "isic_id", "cbo_id"], how="outer").fillna(0)
        
    # print out file
    print "writing to file..."
    new_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybio_required.tsv.bz2'))
    ybio.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=False)
    
    if delete_previous_file:
        print "deleting previous file"
        os.remove(file.name)
    

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;