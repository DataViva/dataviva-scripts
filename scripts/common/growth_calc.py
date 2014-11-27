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
import re
file_path = os.path.dirname(os.path.realpath(__file__))
utils_path = os.path.abspath(os.path.join(file_path, ".."))
sys.path.insert(0, utils_path)
from helpers import get_file


def parse_table_name(t):
    pattern = re.compile('(\w+)(_\w+)*.tsv(.bz2)*')
    m = pattern.search(t)
    return m.group(1)

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
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main(orig_path_str, new_path_str, cols, output_path, years=1):
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
    
    output_path = "/tmp" # - for testing only
    print output_path
    t_name = parse_table_name(new_path_str) # str(os.path.dirname(os.path.realpath( new_path_str ))).split("_")[0]
    print "GOT TABLE NAME OF ", t_name
    if not t_name:
        t_name = "noname"
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
