# -*- coding: utf-8 -*-
"""

run this: python -m scripts.secex.step_6_yp_rca -y YEAR

"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, time, bz2
from collections import defaultdict
from os import environ
import pandas as pd
import pandas.io.sql as sql
from ..config import DATA_DIR
from ..helpers import get_file, format_runtime
from scripts import YEAR, DELETE_PREVIOUS_FILE

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=environ["DATAVIVA_DB_USER"], 
                        passwd=environ["DATAVIVA_DB_PW"], 
                        db=environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def get_brazil_rcas(year):
    
    '''Get world values from database'''
    q = "select year, hs_id, rca from comtrade_ypw where year = {0} and "\
        "wld_id = 'sabra'".format(year)
    bra_rcas = sql.read_frame(q, db, index_col=["year", "hs_id"])
    return bra_rcas

def main(year, delete_previous_file):
    print year
    
    print "loading yp..."
    yp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yp_pcis_diversity.tsv'))
    yp_file = get_file(yp_file_path)
    yp = pd.read_csv(yp_file, sep="\t", converters={"hs_id": str})
    yp = yp.set_index(["year", "hs_id"])
    
    brazil_rcas = get_brazil_rcas(year)
    
    yp["rca"] = brazil_rcas["rca"]
    
    # print out files
    new_yp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yp_pcis_diversity_rcas.tsv.bz2'))
    print ' writing file:', new_yp_file_path
    yp.to_csv(bz2.BZ2File(new_yp_file_path, 'wb'), sep="\t", index=True)
    
    if delete_previous_file:
        print "deleting previous file"
        os.remove(yp_file.name)

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;