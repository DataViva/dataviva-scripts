# -*- coding: utf-8 -*-
"""
    Calculate growth for all tables
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    python -m scripts.secex.step_8_growth \
                -y 2001 \
                -t yb \
                data/secex/export/2001 \
                data/secex/export/2000


"""

''' Import statements '''
import csv, sys, os, math, time, bz2, click
import pandas as pd
import numpy as np
from ..helpers import get_file, format_runtime
from ..growth_lib import growth
# from scripts import YEAR, DELETE_PREVIOUS_FILE, TABLE, help_text_table

def growth(year, delete, table, data_dir, data_dir_prev, data_dir_prev_5):
    
    if not data_dir_prev and not data_dir_prev_5:
        sys.exit("[ERROR]: Need previous year directory for calculating growth.")
    
    index_lookup = {"b":"bra_id", "p":"hs_id", "w":"wld_id"}
    file_lookup = {"yb":"yb_ecis_diversity.tsv.bz2", "ybp":"ybp_rcas_dist_opp.tsv.bz2", \
                    "ybpw": "ybpw.tsv.bz2", "ybw": "ybw.tsv.bz2", \
                    "yp": "yp_pcis_diversity_rcas.tsv.bz2", \
                    "ypw": "ypw.tsv.bz2", "yw": "yw_ecis_diversity.tsv.bz2"}
    index_cols = [index_lookup[i] for i in table if i != "y"]
    converters = {"hs_id": str} if "p" in table else None
    prev_year = str(int(year) - 1)
    prev_year_5 = str(int(year) - 5)
    
    print "loading current year"
    current_file_path = os.path.abspath(os.path.join(data_dir, file_lookup[table]))
    current_file = get_file(current_file_path)
    if not current_file:
        f = os.path.basename(file_lookup[table]).split(".")[0] + "_growth.tsv.bz2"
        current_file_path = os.path.abspath(os.path.join(data_dir, f))
        current_file = get_file(current_file_path)
        if not current_file:
            print "Unable to find", current_file_path
            sys.exit()
    current = pd.read_csv(current_file, sep="\t", converters=converters)
    current = current.set_index(index_cols)
    print index_cols
    if len(index_cols) > 1:
        current = current.sortlevel()
    
    if data_dir_prev:
        print "loading previous year"
        prev_file_path = os.path.abspath(os.path.join(data_dir_prev, file_lookup[table]))
        prev_file = get_file(prev_file_path)
        if not prev_file:
            f = os.path.basename(file_lookup[table]).split(".")[0] + "_growth.tsv.bz2"
            prev_file_path = os.path.abspath(os.path.join(data_dir_prev, f))
            prev_file = get_file(prev_file_path)
            if not prev_file:
                print "Unable to find", prev_file_path
                sys.exit()
        prev = pd.read_csv(prev_file, sep="\t", converters=converters)
        prev = prev.set_index(index_cols)
        if len(index_cols) > 1:
            prev = prev.sortlevel()
            
        # print current.head()
        # print prev.head()
        # sys.exit()
    
        print "calculating 1 year val_usd growth value"
        s = time.time()
        current["val_usd_growth_val"] = current["val_usd"] - prev["val_usd"]
        print (time.time() - s) / 60
    
        print "calculating 1 year val_usd growth rate"
        s = time.time()
        current["val_usd_growth_rate"] = (current["val_usd"] / prev["val_usd"]) - 1
        print (time.time() - s) / 60
    
    if data_dir_prev_5:
        prev_5_file_path = os.path.abspath(os.path.join(data_dir_prev_5, file_lookup[table]))
        prev_5_file = get_file(prev_5_file_path)
        if not prev_5_file:
            f = os.path.basename(file_lookup[table]).split(".")[0] + "_growth.tsv.bz2"
            prev_5_file_path = os.path.abspath(os.path.join(data_dir_prev_5, f))
            prev_5_file = get_file(prev_5_file_path)
        if prev_5_file:
            print "loading file for 5 years ago"
            prev_5 = pd.read_csv(prev_5_file, sep="\t", converters=converters)
            prev_5 = prev_5.set_index(index_cols)
            if len(index_cols) > 1:
                prev_5 = prev_5.sortlevel()
        
            print "calculating 5 year val_usd growth value"
            current["val_usd_growth_val_5"] = current["val_usd"] - prev_5["val_usd"]
    
            print "calculating 5 year val_usd growth rate"
            current["val_usd_growth_rate_5"] = (current["val_usd"] / prev_5["val_usd"]) ** (1.0/5.0) - 1
    
    current[current == np.inf] = np.nan
    
    new_file_name = os.path.basename(file_lookup[table]).split(".")[0] + "_growth.tsv.bz2"
    print "writing new growth file..."
    new_file_path = os.path.abspath(os.path.join(data_dir, new_file_name))
    current.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)
    
    if delete:
        print "deleting previous file"
        os.remove(current_file.name)

@click.command()
@click.argument('data_dir', type=click.Path(exists=True), required=True)
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.option('--delete', '-d', help='Delete the previous file?', is_flag=True, default=False)
@click.option('--table', '-t', help='The year of the data.', type=click.Choice(['all', 'yb', 'ybp', 'ybpw', 'ybw', 'yp', 'ypw', 'yw']), required=True, prompt=True)
@click.option('--data_dir_prev', '-g', type=click.Path(exists=True), required=False)
@click.option('--data_dir_prev_5', '-g5', type=click.Path(exists=True), required=False)
def main(data_dir, year, delete, table, data_dir_prev, data_dir_prev_5):
    
    if table == "all":
        for t in ['yb', 'ybp', 'ybpw', 'ybw', 'yp', 'ypw', 'yw']:
            print; print "table: {0}".format(t); print;
            growth(year, delete, t, data_dir, data_dir_prev, data_dir_prev_5)
    else:
        growth(year, delete, table, data_dir, data_dir_prev, data_dir_prev_5)

if __name__ == "__main__":
    start = time.time()
    
    main()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;