# -*- coding: utf-8 -*-
"""
"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, time, bz2
from collections import defaultdict
from os import environ
import pandas as pd
import pandas.io.sql as sql
import numpy as np
from ..config import DATA_DIR
from ..helpers import get_file
from ..growth_lib import growth
from scripts import YEAR, DELETE_PREVIOUS_FILE

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=environ["DATAVIVA_DB_USER"], 
                        passwd=environ["DATAVIVA_DB_PW"], 
                        db=environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def get_ybp_wld_rcas(geo_level, year):
    
    def rca(bra_tbl, wld_tbl):
        col_sums = bra_tbl.sum(axis=1)
        col_sums = col_sums.reshape((len(col_sums), 1))
        rca_numerator = np.divide(bra_tbl, col_sums)
    
        row_sums = wld_tbl.sum(axis=0)
        total_sum = wld_tbl.sum().sum()
        rca_denominator = row_sums / total_sum
        rcas = rca_numerator / rca_denominator
        return rcas

    '''Get domestic values from TSV'''
    ybp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'ybp.tsv'))
    ybp_file = get_file(ybp_file_path)
    ybp_dom = pd.read_csv(ybp_file, sep="\t", converters={"hs_id":str})
    hs_criterion = ybp_dom['hs_id'].map(lambda x: len(x) == 6)
    bra_criterion = ybp_dom['bra_id'].map(lambda x: len(x) == geo_level)
    ybp_dom = ybp_dom[hs_criterion & bra_criterion]
    ybp_dom = ybp_dom.drop(["year"], axis=1)
    ybp_dom = ybp_dom.pivot(index="bra_id", columns="hs_id", values="val_usd").fillna(0)
    
    '''Get world values from database'''
    q = "select wld_id, hs_id, val_usd from comtrade_ypw where year = {0}".format(year)
    ybp_wld = sql.read_frame(q, db)
    ybp_wld = ybp_wld.pivot(index="wld_id", columns="hs_id", values="val_usd")
    ybp_wld = ybp_wld.reindex(columns=ybp_dom.columns)
    ybp_wld = ybp_wld.fillna(0)
    
    mcp = rca(ybp_dom, ybp_wld).fillna(0)
    mcp[mcp == np.inf] = 0
    
    return mcp

def get_ybp_domestic_rcas(geo_level, year):
    ybp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'ybp.tsv'))
    ybp_file = get_file(ybp_file_path)
    ybp = pd.read_csv(ybp_file, sep="\t", converters={"hs_id":str})
    
    hs_criterion = ybp['hs_id'].map(lambda x: len(x) == 6)
    bra_criterion = ybp['bra_id'].map(lambda x: len(x) == geo_level)
    
    ybp = ybp[hs_criterion & bra_criterion]
    ybp = ybp.drop(["year"], axis=1)
    
    ybp = ybp.pivot(index="bra_id", columns="hs_id", values="val_usd").fillna(0)
    
    rcas = growth.rca(ybp)
    
    return rcas

def get_pcis(geo_level, year):
    yp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yp_pcis_diversity_rcas.tsv'))
    yp_file = get_file(yp_file_path)
    yp = pd.read_csv(yp_file, sep="\t", converters={"hs_id":str})
    
    hs_criterion = yp['hs_id'].map(lambda x: len(x) == 6)
    
    yp = yp[hs_criterion]
    yp = yp.drop(["year", "val_usd"], axis=1)
    yp = yp.set_index("hs_id")
    yp = yp[pd.notnull(yp.pci)]
    
    return yp["pci"]

def get_wld_proximity(year):

    '''Get values from database'''
    q = "select wld_id, hs_id, val_usd " \
        "from comtrade_ypw " \
        "where year = {0} and length(hs_id) = 6".format(year)
    table = sql.read_frame(q, db)
    table = table.pivot(index="wld_id", columns="hs_id", values="val_usd")
    table = table.fillna(0)

    '''Use growth library to run RCA calculation on data'''
    mcp = growth.rca(table)
    mcp[mcp >= 1] = 1
    mcp[mcp < 1] = 0
    
    prox = growth.proximity(mcp)

    return prox

def main(year, delete_previous_file):
    
    ypw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'comtrade', year, 'ypw.tsv'))
    ypw_file = get_file(ypw_file_path)
    ypw = pd.read_csv(ypw_file, sep="\t", converters={"hs_id":str})
    
    '''
        RCAS
    '''
    ypw_rcas = ypw.copy()
    ypw_rcas = ypw_rcas.drop(["year"], axis=1)
    ypw_rcas = ypw_rcas.pivot(index="wld_id", columns="hs_id", values="val_usd").fillna(0)
    rcas = growth.rca(ypw_rcas)
    
    # print rcas.ix["sabra"]
    rcas = pd.DataFrame(rcas.stack(), columns=["rca"])
    # print rcas.head(20)
    # sys.exit()
    
    ypw = ypw.set_index(["wld_id", "hs_id"])
    ypw["rca"] = rcas
    ypw = ypw.set_index(["year"], append=True)
    ypw = ypw.reorder_levels(["year", "wld_id", "hs_id"])
    
    new_ypw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'comtrade', year, 'ypw_rcas.tsv.bz2'))
    print ' writing file:', new_ypw_file_path
    ypw.to_csv(bz2.BZ2File(new_ypw_file_path, 'wb'), sep="\t", index=True)
    
    sys.exit()
    
    
    '''
        SET RCAS TO NULL
    '''
    rcas_dom = rcas_dom.replace(0, np.nan)
    rcas_wld = rcas_wld.replace(0, np.nan)
    
    
    def tryto(df, col, ind):
        if col in df.columns:
            if ind in df.index:
                return df[col][ind]
        return None
    
    for bra in set(rcas_dom.index).union(set(rcas_wld.index)):
        for hs in set(rcas_dom.columns).union(set(rcas_wld.columns)):
            rca_dist_opp.append([year, bra, hs, \
                            tryto(rcas_dom, hs, bra), tryto(rcas_wld, hs, bra), \
                            tryto(dist_dom, hs, bra), tryto(dist_wld, hs, bra), \
                            tryto(opp_gain_dom, hs, bra), tryto(opp_gain_wld, hs, bra) ])
    
    print len(rca_dist_opp), "rows updated"
    
    
    # now time to merge!
    print "merging datasets..."
    ybp_rdo = pd.DataFrame(rca_dist_opp, columns=["year", "bra_id", "hs_id", "rca", "rca_wld", "distance", "distance_wld", "opp_gain", "opp_gain_wld"])
    ybp_rdo["year"] = ybp_rdo["year"].astype(int)
    ybp_rdo["hs_id"] = ybp_rdo["hs_id"].astype(str)
    ybp_rdo = ybp_rdo.set_index(["year", "bra_id", "hs_id"])
    
    # get union of both sets of indexes
    all_ybp_indexes = set(ybp.index).union(set(ybp_rdo.index))
    
    all_ybp_indexes = pd.MultiIndex.from_tuples(all_ybp_indexes, names=["year", "bra_id", "hs_id"])
    ybp = ybp.reindex(index=all_ybp_indexes)
    ybp["rca"] = ybp_rdo["rca"]
    ybp["rca_wld"] = ybp_rdo["rca_wld"]
    ybp["distance"] = ybp_rdo["distance"]
    ybp["distance_wld"] = ybp_rdo["distance_wld"]
    ybp["opp_gain"] = ybp_rdo["opp_gain"]
    ybp["opp_gain_wld"] = ybp_rdo["opp_gain_wld"]
    
    # print out file
    new_ybp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'ybp_rcas_dist_opp.tsv.bz2'))
    print ' writing file:', new_ybp_file_path
    ybp.to_csv(bz2.BZ2File(new_ybp_file_path, 'wb'), sep="\t", index=True)
    
    if delete_previous_file:
        print "deleting previous file"
        os.remove(ybp_file.name)

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;