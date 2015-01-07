# -*- coding: utf-8 -*-
from __future__ import print_function
"""
    Test DB vars
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python \
    -m scripts.secex_new.test_db_vars \
    data/secex/export/ \
    data/secex/import/ \
    -y 2000 \
    -ey 2014 \
    -d

"""

''' Import statements '''
import os, sys, time, bz2, click, urllib, json
import pandas as pd
import pandas.io.sql as sql
import numpy as np

from _to_df import to_df
from _merge import merge
from _replace_vals import replace_vals

@click.command()
@click.argument('export_file_path', type=click.Path(exists=True))
@click.argument('import_file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True, type=int)
@click.option('-ey', '--end_year', type=int)
@click.option('-o', '--output_file', type=click.Path(), required=False)
@click.option('-d', '--debug', is_flag=True)
def main(export_file_path, import_file_path, year, end_year, output_file, debug):
    # print export_file_path, import_file_path, year, end_year, debug
    if output_file:
        output_file = open(output_file, 'w')

    end_year = end_year if end_year else year
    years = range(year, end_year+1)
    missing = {}
    for y in years:
        if debug:
            print; print(y); print;
    
        if debug:
            print('STEP 1: \nImport file to pandas dataframe')
        export_file_path_year = os.path.join(export_file_path, "MDIC_{0}.csv.zip".format(y))
        import_file_path_year = os.path.join(import_file_path, "MDIC_{0}.csv.zip".format(y))
        secex_exports = to_df(export_file_path_year, False, debug)
        secex_imports = to_df(import_file_path_year, False, debug)
        # secex_exports = secex_exports.head(10000)
        # secex_imports = secex_imports.head(10000)
    
        if debug:
            print('STEP 2: \nMerge imports and exports')
        secex_df = merge(secex_imports, secex_exports, debug)
    
        if debug:
            print('STEP 3: \nReplace vals with DB IDs')
        missing = replace_vals(secex_df, missing, debug)
    
    print("The following values are missing from the DB but found in the data:", file=output_file)
    for col, missing_vals in missing.items():
        print("\nColumn: {0}".format(col), file=output_file)
        for m in list(missing_vals):
            print(m, file=output_file)
    

if __name__ == "__main__":
    start = time.time()

    main()