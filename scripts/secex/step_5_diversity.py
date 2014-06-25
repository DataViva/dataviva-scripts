# -*- coding: utf-8 -*-
"""
    YB table to add unique industries and occupations to DB
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This script will calculate the number of unique products (hs)
    and destinations (wld) that are found in each location (state, meso,
    planning region and municipality).
    
    run this:
    python -m scripts.secex.step_5_diversity \
                -y 2000 \
                data/secex/export/2000
    
"""

''' Import statements '''
import csv, sys, os, argparse, math, time, bz2, click
import pandas as pd
import numpy as np
from ..helpers import get_file, format_runtime
from ..growth_lib import growth

def get_deepest(column):
    if column == "hs_id": return 6
    if column == "bra_id": return 8
    if column == "wld_id": return 5

def get_diversity(file, index, column):
    if index == "hs_id" or column == "hs_id":
        tbl = pd.read_csv(file, sep="\t", converters={"hs_id": str})
    else:
        tbl = pd.read_csv(file, sep="\t")

    # filter table by deepest length
    criterion = tbl[column].map(lambda x: len(x) == get_deepest(column))
    tbl = tbl[criterion]
    
    diversity = tbl.pivot(index=index, columns=column, values="val_usd").fillna(0)
    diversity[diversity >= 1] = 1
    diversity[diversity < 1] = 0
    diversity = diversity.sum(axis=1)
    
    return diversity

def get_effective_diversity(file, index, column):
    if index == "hs_id" or column == "hs_id":
        tbl = pd.read_csv(file, sep="\t", converters={"hs_id": str})
    else:
        tbl = pd.read_csv(file, sep="\t")
    
    # filter table by deepest length
    criterion = tbl[column].map(lambda x: len(x) == get_deepest(column))
    tbl = tbl[criterion]
    
    entropy = tbl.pivot(index=index, columns=column, values="val_usd").fillna(0)
    entropy = entropy.T / entropy.T.sum()
    entropy = entropy * np.log(entropy)
    
    entropy = entropy.sum() * -1
    es = pd.Series([np.e]*len(entropy), index=entropy.index)
    diversity_eff = es ** entropy
    
    return diversity_eff

@click.command()
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.option('--delete', '-d', help='Delete the previous file?', is_flag=True, default=False)
@click.argument('data_dir', type=click.Path(exists=True), required=True)
def main(year, delete, data_dir):
    
    '''
        YB Diversity
    '''
    # start with unique hs / bra
    ybp_file_path = os.path.abspath(os.path.join(data_dir, 'ybp.tsv.bz2'))
    ybp_file = get_file(ybp_file_path)
    yb_hs_diversity = get_diversity(ybp_file, "bra_id", "hs_id")
    ybp_file.seek(0)
    yb_hs_effective_diversity = get_effective_diversity(ybp_file, "bra_id", "hs_id")
    
    # unique wld / bra
    ybw_file_path = os.path.abspath(os.path.join(data_dir, 'ybw.tsv.bz2'))
    ybw_file = get_file(ybw_file_path)
    yb_wld_diversity = get_diversity(ybw_file, "bra_id", "wld_id")
    ybw_file.seek(0)
    yb_wld_effective_diversity = get_effective_diversity(ybw_file, "bra_id", "wld_id")
    
    yb_file_path = os.path.abspath(os.path.join(data_dir, 'yb_ecis.tsv.bz2'))
    yb_file = get_file(yb_file_path)
    yb_diversity = pd.read_csv(yb_file, sep="\t", index_col=["bra_id"])
    
    yb_diversity["hs_diversity"] = yb_hs_diversity
    yb_diversity["hs_diversity_eff"] = yb_hs_effective_diversity
    yb_diversity["wld_diversity"] = yb_wld_diversity
    yb_diversity["wld_diversity_eff"] = yb_wld_effective_diversity
    
    # print out file
    print "writing yb file..."
    new_yb_file_path = os.path.abspath(os.path.join(data_dir, 'yb_ecis_diversity.tsv.bz2'))
    yb_diversity.to_csv(bz2.BZ2File(new_yb_file_path, 'wb'), sep="\t", index=True)
    
    
    
    '''
        YP Diversity
    '''
    ypw_file_path = os.path.abspath(os.path.join(data_dir, 'ypw.tsv.bz2'))
    ypw_file = get_file(ypw_file_path)
    
    
    yp_wld_diversity = get_diversity(ypw_file, "hs_id", "wld_id")
    ypw_file.seek(0)
    yp_wld_effective_diversity = get_effective_diversity(ypw_file, "hs_id", "wld_id")
    ybp_file.seek(0)
    yp_bra_diversity = get_diversity(ybp_file, "hs_id", "bra_id")
    ybp_file.seek(0)
    yp_bra_effective_diversity = get_effective_diversity(ybp_file, "hs_id", "bra_id")
        
    # print out file
    yp_file_path = os.path.abspath(os.path.join(data_dir, 'yp_pcis.tsv.bz2'))
    yp_file = get_file(yp_file_path)
    yp_diversity = pd.read_csv(yp_file, sep="\t", converters={"hs_id":str})
    yp_diversity = yp_diversity.set_index(["hs_id"])
    
    yp_diversity["wld_diversity"] = yp_wld_diversity
    yp_diversity["wld_diversity_eff"] = yp_wld_effective_diversity
    yp_diversity["bra_diversity"] = yp_bra_diversity
    yp_diversity["bra_diversity_eff"] = yp_bra_effective_diversity
    
    print "writing yp file..."
    new_yp_file_path = os.path.abspath(os.path.join(data_dir, 'yp_pcis_diversity.tsv.bz2'))
    yp_diversity.to_csv(bz2.BZ2File(new_yp_file_path, 'wb'), sep="\t", index=True)
    
    
    
    '''
        YW Diversity
    '''
    # unique isic / cbo
    ypw_file.seek(0)
    yw_hs_diversity = get_diversity(ypw_file, "wld_id", "hs_id")
    ypw_file.seek(0)
    yw_hs_effective_diversity = get_effective_diversity(ypw_file, "wld_id", "hs_id")
    ybw_file.seek(0)
    yw_bra_diversity = get_diversity(ybw_file, "wld_id", "bra_id")
    ybw_file.seek(0)
    yw_bra_effective_diversity = get_effective_diversity(ybw_file, "wld_id", "bra_id")
    
    # print out file
    yw_file_path = os.path.abspath(os.path.join(data_dir, 'yw_ecis.tsv.bz2'))
    yw_file = get_file(yw_file_path)
    yw_diversity = pd.read_csv(yw_file, sep="\t")
    yw_diversity = yw_diversity.set_index(["wld_id"])
    
    yw_diversity["hs_diversity"] = yw_hs_diversity
    yw_diversity["hs_diversity_eff"] = yw_hs_effective_diversity
    yw_diversity["bra_diversity"] = yw_bra_diversity
    yw_diversity["bra_diversity_eff"] = yw_bra_effective_diversity
    
    print "writing yw file..."
    new_yw_file_path = os.path.abspath(os.path.join(data_dir, 'yw_ecis_diversity.tsv.bz2'))
    yw_diversity.to_csv(bz2.BZ2File(new_yw_file_path, 'wb'), sep="\t", index=True)
    
    if delete:
        print "deleting previous file"
        os.remove(yb_file.name)
        os.remove(yp_file.name)
        os.remove(yw_file.name)
    
if __name__ == "__main__":
    start = time.time()
    
    main()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;