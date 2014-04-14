# -*- coding: utf-8 -*-
"""

how to run this: python -m scripts.secex.step_3_pci_wld_eci -y YEAR

"""

''' Import statements '''
import csv, sys, os, argparse, bz2, time
import pandas as pd
from collections import defaultdict
from os import environ
from ..config import DATA_DIR
from ..helpers import get_file, format_runtime
from scripts import YEAR, DELETE_PREVIOUS_FILE

def main(year, delete_previous_file):
    
    pci_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', 'observatory_pcis.csv'))
    pci_file = get_file(pci_file_path)
    pcis = pd.read_csv(pci_file, sep=",", converters={"hs_id": str})
    pcis = pcis.set_index(["year", "hs_id"])
    
    eci_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', 'observatory_ecis.csv'))
    eci_file = get_file(eci_file_path)
    ecis = pd.read_csv(eci_file, sep=",")
    ecis = ecis.set_index(["year", "wld_id"])

    yp_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yp.tsv'))
    yp_file = get_file(yp_file_path)
    yp = pd.read_csv(yp_file, sep="\t", converters={"hs_id": str})
    yp = yp.set_index(["year", "hs_id"])
    
    yw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, 'yw.tsv'))
    yw_file = get_file(yw_file_path)
    yw = pd.read_csv(yw_file, sep="\t", converters={"wld_id": str})
    yw = yw.set_index(["year", "wld_id"])
    
    yp["pci"] = pcis["pci"]
    yw["eci"] = ecis["eci"]
    
    '''
        write new files
    '''
    new_yp_file = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, "yp_pcis.tsv.bz2"))
    print ' writing file:', new_yp_file
    yp.to_csv(bz2.BZ2File(new_yp_file, 'wb'), sep="\t", index=True)
    
    new_yw_file = os.path.abspath(os.path.join(DATA_DIR, 'secex', year, "yw_ecis.tsv.bz2"))
    print ' writing file:', new_yw_file
    yw.to_csv(bz2.BZ2File(new_yw_file, 'wb'), sep="\t", index=True)
    
    '''
        delete old files
    '''
    if delete_previous_file:
        print "deleting previous files"
        os.remove(yp_file.name)
        os.remove(yw_file.name)
        

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;