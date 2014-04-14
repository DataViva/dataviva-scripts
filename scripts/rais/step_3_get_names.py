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
import csv, sys, os, argparse, math, time, bz2, MySQLdb
from collections import defaultdict
from os import environ
import pandas as pd
import numpy as np
from ..helpers import get_file
from ..config import DATA_DIR
from ..growth_lib import growth
from scripts import YEAR, DELETE_PREVIOUS_FILE
import pandas.io.sql as sql

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=environ["DATAVIVA_DB_USER"], 
                        passwd=environ["DATAVIVA_DB_PW"], 
                        db=environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def main(year, delete_previous_file):
    for gl in ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]:
    
        fname = "required_" + gl + "_.tsv"
        file_path = os.path.abspath(os.path.join("/Users/alexandersimoes/Sites/dataviva_scripts", fname))
        file = get_file(file_path)
        req = pd.read_csv(file, sep="\t", converters={"cbo_id":str})
        req = req.set_index(["cbo_id"])
    
        q = "select name_en, id as cbo_id from attrs_cbo where length(id)=4"
        names = sql.read_frame(q, db, index_col=["cbo_id"])
    
        req["name"] = names["name_en"]
    
        req.to_csv(file_path, sep="\t")

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR, DELETE_PREVIOUS_FILE)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;
    