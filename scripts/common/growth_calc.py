# compute_growth.py
# -*- coding: utf-8 -*-
"""
    Format RAIS data for DB entry
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Columns:
    0 Municipality_ID
    1 EconmicAtivity_ID_ISIC
    2 EconomicActivity_ID_CNAE
    3 BrazilianOcupation_ID
    4 AverageMonthlyWage
    5 WageReceived
    6 Employee_ID
    7 Establishment_ID
    8 Year
    
    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python scripts/common/growth_calc.py file.tsv.bz2 file2.tsv.bz2 --years=1 --cols=wage,age

"""

''' Import statements '''
import os, sys, time, bz2, click
import pandas as pd
import numpy as np
file_path = os.path.dirname(os.path.realpath(__file__))
utils_path = os.path.abspath(os.path.join(file_path, ".."))
sys.path.insert(0, utils_path)
from helpers import get_file

def do_growth(tbl, tbl_prev, cols, years_ago=1):

    '''Growth rate'''
    for orig_col_name in cols:
        print tbl.columns,  tbl_prev.columns
        new_col_name = orig_col_name + "_growth"
        if years_ago > 1:
            new_col_name = "{0}_{1}".format(new_col_name, years_ago)
        tbl[new_col_name] = (tbl[orig_col_name] / tbl_prev[orig_col_name]) ** (1.0/years_ago) - 1
    
    return tbl

@click.command()
@click.argument('orig_path_str', type=click.Path(exists=True))
@click.argument('new_path_str', type=click.Path(exists=True))
@click.option('-c', '--cols', prompt='Columns separated by commas to compute growth', type=str, required=True)
@click.option('-y', '--years', prompt='years between data points', type=int, required=False)
def main(orig_path_str, new_path_str, cols, years=1):
    start = time.time()
    step = 0
    
    step+=1; print; print '''STEP {0}: \nCalculate 1 year growth'''.format(step)
    
    orig_path = get_file(orig_path_str)
    df1 = pd.read_csv(orig_path, sep="\t")
    new_path = get_file(new_path_str)
    df2 = pd.read_csv(new_path, sep="\t")

    col_names = cols.split(",")
    print "CALCULATING growth for the following columns:", col_names

    df2 = do_growth(df1, df2, col_names, years)
    
    output_path = "./" # - for testing only

    t_name = str(os.path.dirname(os.path.realpath( new_path_str ))).split("_")[0]

    new_file_path = os.path.abspath(os.path.join(output_path, "{0}_with_growth.tsv.bz2".format(t_name)))
    df2.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.2f")
    
    print("--- %s minutes ---" % str((time.time() - start)/60))

if __name__ == "__main__":
    start = time.time()

    main()
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;
