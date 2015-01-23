# compute_growth.py
# -*- coding: utf-8 -*-
"""
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python scripts/common/growth_calc.py file_2007.tsv.bz2 file_2008.tsv.bz2 --years=1 --cols=wage,age -o /tmp

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
    pattern = re.compile('(\w+).tsv(.bz2)*')
    
    # t = t.replace('_with_growth', '')
    m = pattern.search(t)
    if m:
        return m.group(1)
         
def do_growth(t_name, tbl, tbl_prev, cols, years_ago=1, edu_mode='hedu'):
    '''Growth rate'''
    print "COLS", cols
    pk_lookup = {"b": "bra_id", "s": "school_id", "u": "university_id", "d": "d_id", "c": "course_%s_id" % (edu_mode) }
    t_namelook = t_name.split("_")[0]    
    pk = [pk_lookup[letter] for letter in t_namelook if letter != 'y']

    tbl = pd.merge(tbl, tbl_prev[pk + cols], how='left', left_on=pk, right_on=pk )
    
    for orig_col_name in cols:
        print "DOING", orig_col_name
        print tbl.columns,  tbl_prev.columns

        new_col_name = orig_col_name + "_growth"
        if years_ago > 1:
            new_col_name = "{0}_{1}".format(new_col_name, years_ago)
            
            
        tbl[new_col_name] = ((1.0 * tbl[orig_col_name+"_x"]) / (1.0*tbl[orig_col_name + "_y"])) ** (1.0/years_ago) - 1
    for colname in cols:
        del tbl[colname + "_y"]
        tbl.rename(columns={ c+"_x" : c for c in cols }, inplace=True)
        # tbl[new_col_name] = tbl3[new_col_name] # -- this is safe because of the left merge
    return tbl

@click.command()
@click.argument('second_year_str', type=click.Path(exists=True))
@click.option('-r', '--replacer', prompt='value to be replaced', type=str, required=True)
@click.option('-c', '--cols', prompt='Columns separated by commas to compute growth', type=str, required=True)
@click.option('-y', '--years', prompt='years between data points (seperate by comma for multiple years)', type=str, required=True)
@click.option('-e', '--edu', prompt='hedu or sc', type=str, required=True)
@click.option('-s', '--strcasts', prompt='Columns separed by commas to treat as strings', type=str, required=False)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main(second_year_str, replacer, cols, output_path, edu, years=1, strcasts=""):
    start = time.time()
    step = 0

    strcastlist = strcasts.split(",")
    converters = {x:str for x in strcastlist}
    
    step+=1; print; print '''STEP {0}: \nCalculate 1 year growth'''.format(step)
    
    new_path = get_file(second_year_str)
    df2 = pd.read_csv(new_path, sep="\t", converters=converters)

    col_names = cols.split(",")
    print "CALCULATING growth for the following columns:", col_names
    t_name = parse_table_name(second_year_str) 

    years = years.split(",")
    for y in years:
        y = int(y)
        target_str = second_year_str.replace(replacer, str(int(replacer) - y))
        orig_path = get_file(target_str)
        df1 = pd.read_csv(orig_path, sep="\t", converters=converters)
        df2 = do_growth(t_name, df2, df1, col_names, y, edu)
    
    
    print "GOT TABLE NAME OF ", t_name
    if not t_name:
        t_name = "noname"
    new_file_path = os.path.abspath(os.path.join(output_path, "{0}_with_growth.tsv.bz2".format(t_name)))
    df2.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=False, float_format="%.2f")
    
    print("--- %s minutes ---" % str((time.time() - start)/60))

if __name__ == "__main__":
    start = time.time()

    main()
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;
