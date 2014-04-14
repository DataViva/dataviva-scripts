# -*- coding: utf-8 -*-
"""
    Calculate exclusivity for a given occupation in a given year
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

def get_all_cbo(year):
    yo_file_names = ['yo.tsv', 'yo_diversity.tsv', 'yo_diversity_growth.tsv']
    for yo_file_name in yo_file_names:
        yo_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, yo_file_name))
        yo_file = get_file(yo_file_path)
        if yo_file:
            break

    yo = pd.read_csv(yo_file, sep="\t", converters={"cbo_id":str})
    yo = yo.set_index(["cbo_id"])
    cbos = [cbo for cbo in list(yo.index) if len(cbo) == 4]
    
    return cbos

def get_ybi_rcas(year, geo_level):
    ybi_file_names = ['ybi.tsv', 'ybi_rcas_dist_opp.tsv', 'ybi_rcas_dist_opp_growth.tsv']
    for ybi_file_name in ybi_file_names:
        ybi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, ybi_file_name))
        ybi_file = get_file(ybi_file_path)
        if ybi_file:
            break
    
    ybi = pd.read_csv(ybi_file, sep="\t")
    
    isic_criterion = ybi['isic_id'].map(lambda x: len(x) == 5)
    bra_criterion = ybi['bra_id'].map(lambda x: len(x) == geo_level)
    
    ybi = ybi[isic_criterion & bra_criterion]
    ybi = ybi[["bra_id", "isic_id", "wage"]]
    
    ybi = ybi.pivot(index="bra_id", columns="isic_id", values="wage").fillna(0)
    
    rcas = growth.rca(ybi)
    rcas[rcas >= 1] = 1
    rcas[rcas < 1] = 0
    
    return rcas

def get_ybio(year):
    print "loading YBIO..."
    ybio_file_names = ['ybio.tsv', 'ybio_required.tsv', 'ybio_required_growth.tsv']
    for ybio_file_name in ybio_file_names:
        ybio_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, ybio_file_name))
        ybio_file = get_file(ybio_file_path)
        if ybio_file:
            break
    
    ybio = pd.read_csv(ybio_file, sep="\t", converters={"year": str, "cbo_id":str})
    isic_criterion = ybio['isic_id'].map(lambda x: len(x) == 5)
    cbo_criterion = ybio['cbo_id'].map(lambda x: len(x) == 4)
    bra_criterion = ybio['bra_id'].map(lambda x: len(x) == 8)
    ybio = ybio[isic_criterion & cbo_criterion & bra_criterion]
    
    ybio = ybio[["isic_id", "cbo_id", "bra_id", "num_emp"]]
    print "pivoting YBIO..."
    ybio = ybio.pivot_table(rows=["isic_id", "cbo_id"], cols="bra_id", values="num_emp")
    ybio = ybio.fillna(0)
    
    panel = ybio.to_panel()
    panel = panel.swapaxes("items", "minor")
    panel = panel.swapaxes("major", "minor")
    
    return panel

def main(year, delete_previous_file):
    start = time.time()
    all_cbo = get_all_cbo(year)
    
    '''get ybi RCAs'''
    rcas = get_ybi_rcas(year, 8)
    
    denoms = rcas.sum()
    
    # z       = occupations
    # rows    = bras
    # colums  = isics
    ybio = get_ybio(year)
    
    yio_importance = []
    for cbo in all_cbo:
        start = time.time()
        
        try:
            num_emp = ybio[cbo].fillna(0)
        except:
            continue
        numerators = num_emp * rcas
        numerators = numerators.fillna(0)
        
        '''convert nominal num_emp values to 0s and 1s'''
        numerators[numerators >= 1] = 1
        numerators[numerators < 1] = 0
        
        numerators = numerators.sum()
        importance = numerators / denoms
        # print importance
        
        for isic in importance.index:
            imp = importance[isic]
            yio_importance.append([year, isic, cbo, imp])
        
        # print year, cbo, time.time() - start
        sys.stdout.write('\r ' + year + ' CBO id: ' + cbo + ' '*10)
        sys.stdout.flush() # important
    
    # now time to merge!
    print
    print "merging datasets..."
    yio_importance = pd.DataFrame(yio_importance, columns=["year", "isic_id", "cbo_id", "importance"])
    yio_importance = yio_importance.set_index(["year", "isic_id", "cbo_id"])
    
    yio_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yio.tsv'))
    yio_file = get_file(yio_file_path)
    
    yio = pd.read_csv(yio_file, sep="\t", converters={"year": str, "cbo_id": str})
    yio = yio.set_index(["year", "isic_id", "cbo_id"])
    yio["importance"] = yio_importance["importance"]
        
    # print out file
    print "writing to file..."
    new_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yio_importance.tsv.bz2'))
    yio.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)
    
    if delete_previous_file:
        print "deleting previous file"
        os.remove(yio_file.name)

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;