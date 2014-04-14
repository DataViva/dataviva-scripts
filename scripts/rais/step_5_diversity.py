# -*- coding: utf-8 -*-
"""
    YB table to add unique industries and occupations to DB
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This script will calculate the number of unique industries (isic)
    and occupations (cbo) that are found in each location (state, meso,
    planning region and municipality).
"""

''' Import statements '''
import csv, sys, os, argparse, math, time, bz2
from collections import defaultdict
from os import environ
from os.path import basename
import pandas as pd
import numpy as np
from ..helpers import get_file, format_runtime
from ..config import DATA_DIR
from ..growth_lib import growth
from scripts import YEAR, DELETE_PREVIOUS_FILE

def get_unique(file, index, column):
    if "o" in file.name:
        tbl = pd.read_csv(file, sep="\t", converters={"cbo_id": str})
    else:
        tbl = pd.read_csv(file, sep="\t")
    tbl = tbl.pivot(index=index, columns=column, values="wage").fillna(0)

    tbl[tbl >= 1] = 1
    tbl[tbl < 1] = 0
    
    return tbl.sum(axis=1)

def get_deepest(column):
    if column == "bra_id": return 8
    if column == "isic_id": return 5
    if column == "cbo_id": return 4

def get_diversity(file, index, column):
    if index == "cbo_id" or column == "cbo_id":
        tbl = pd.read_csv(file, sep="\t", converters={"cbo_id": str})
    else:
        tbl = pd.read_csv(file, sep="\t")

    # filter table by deepest length
    criterion = tbl[column].map(lambda x: len(x) == get_deepest(column))
    tbl = tbl[criterion]
    
    diversity = tbl.pivot(index=index, columns=column, values="num_emp").fillna(0)
    diversity[diversity >= 1] = 1
    diversity[diversity < 1] = 0
    diversity = diversity.sum(axis=1)
    
    return diversity

def get_effective_diversity(file, index, column):
    if index == "cbo_id" or column == "cbo_id":
        tbl = pd.read_csv(file, sep="\t", converters={"cbo_id": str})
    else:
        tbl = pd.read_csv(file, sep="\t")

    # filter table by deepest length
    criterion = tbl[column].map(lambda x: len(x) == get_deepest(column))
    tbl = tbl[criterion]
    
    entropy = tbl.pivot(index=index, columns=column, values="num_emp").fillna(0)
    entropy = entropy.T / entropy.T.sum()
    entropy = entropy * np.log(entropy)
    
    entropy = entropy.sum() * -1
    es = pd.Series([np.e]*len(entropy), index=entropy.index)
    diversity_eff = es ** entropy
    
    return diversity_eff

def main(year, delete_previous_file):
    
    '''
        YB Diversity
    '''
    # start with isic / bra diversity
    ybi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybi.tsv'))
    ybi_file = get_file(ybi_file_path)
    yb_isic_diversity = get_diversity(ybi_file, "bra_id", "isic_id")
    ybi_file.seek(0)
    yb_isic_effective_diversity = get_effective_diversity(ybi_file, "bra_id", "isic_id")
    
    # cbo / bra diversity
    ybo_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybo.tsv'))
    ybo_file = get_file(ybo_file_path)
    yb_cbo_diversity = get_diversity(ybo_file, "bra_id", "cbo_id")
    ybo_file.seek(0)
    yb_cbo_effective_diversity = get_effective_diversity(ybo_file, "bra_id", "cbo_id")
    
    yb_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yb.tsv'))
    yb_file = get_file(yb_file_path)
    yb_diversity = pd.read_csv(yb_file, sep="\t", index_col=["bra_id"])
    
    yb_diversity["isic_diversity"] = yb_isic_diversity
    yb_diversity["isic_diversity_eff"] = yb_isic_effective_diversity
    yb_diversity["cbo_diversity"] = yb_cbo_diversity
    yb_diversity["cbo_diversity_eff"] = yb_cbo_effective_diversity
    
    # print out file
    print "writing yb file..."
    new_yb_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yb_diversity.tsv.bz2'))
    yb_diversity.to_csv(bz2.BZ2File(new_yb_file_path, 'wb'), sep="\t", index=True)
    
    
    
    '''
        YI Diversity
    '''
    yio_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yio.tsv'))
    yio_file = get_file(yio_file_path)
    
    
    yi_cbo_diversity = get_diversity(yio_file, "isic_id", "cbo_id")
    yio_file.seek(0)
    yi_cbo_effective_diversity = get_effective_diversity(yio_file, "isic_id", "cbo_id")
    ybi_file.seek(0)
    yi_bra_diversity = get_diversity(ybi_file, "isic_id", "bra_id")
    ybi_file.seek(0)
    yi_bra_effective_diversity = get_effective_diversity(ybi_file, "isic_id", "bra_id")
        
    # print out file
    yi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yi.tsv'))
    yi_file = get_file(yi_file_path)
    yi_diversity = pd.read_csv(yi_file, sep="\t")
    yi_diversity = yi_diversity.set_index(["isic_id"])
    
    yi_diversity["cbo_diversity"] = yi_cbo_diversity
    yi_diversity["cbo_diversity_eff"] = yi_cbo_effective_diversity
    yi_diversity["bra_diversity"] = yi_bra_diversity
    yi_diversity["bra_diversity_eff"] = yi_bra_effective_diversity
    
    print "writing yi file..."
    new_yi_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yi_diversity.tsv.bz2'))
    yi_diversity.to_csv(bz2.BZ2File(new_yi_file_path, 'wb'), sep="\t", index=True)
    
    
    
    '''
        YO Diversity
    '''
    # unique isic / cbo
    yio_file.seek(0)
    yo_isic_diversity = get_diversity(yio_file, "cbo_id", "isic_id")
    yio_file.seek(0)
    yo_isic_effective_diversity = get_effective_diversity(yio_file, "cbo_id", "isic_id")
    ybo_file.seek(0)
    yo_bra_diversity = get_diversity(ybo_file, "cbo_id", "bra_id")
    ybo_file.seek(0)
    yo_bra_effective_diversity = get_effective_diversity(ybo_file, "cbo_id", "bra_id")
    
    # print out file
    yo_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yo.tsv'))
    yo_file = get_file(yo_file_path)
    yo_diversity = pd.read_csv(yo_file, sep="\t", converters={"cbo_id": str})
    yo_diversity = yo_diversity.set_index(["cbo_id"])
    
    yo_diversity["isic_diversity"] = yo_isic_diversity
    yo_diversity["isic_diversity_eff"] = yo_isic_effective_diversity
    yo_diversity["bra_diversity"] = yo_bra_diversity
    yo_diversity["bra_diversity_eff"] = yo_bra_effective_diversity
    
    print "writing yo file..."
    new_yo_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'yo_diversity.tsv.bz2'))
    yo_diversity.to_csv(bz2.BZ2File(new_yo_file_path, 'wb'), sep="\t", index=True)
    
    
    if delete_previous_file:
        print "deleting previous file"
        os.remove(yb_file.name)
        os.remove(yi_file.name)
        os.remove(yo_file.name)
    
if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;