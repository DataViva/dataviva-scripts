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
    python scripts/rais_new/format_raw_data.py data/rais/Rais2002.csv.bz2 -y 2002 -o data/rais/2002

"""

''' Import statements '''
import os, sys, time, bz2, click
import pandas as pd
import pandas.io.sql as sql
import numpy as np

from _to_df import to_df
from _replace_vals import replace_vals
from _aggregate import aggregate
from _shard import shard
from _required import required
from _importance import importance
from _calc_diversity import calc_diversity
from _rdo import rdo
from _growth import calc_growth
from _column_lengths import add_column_length

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
@click.option('prev_path', '--prev', '-g', help='Path to files from the previous year for calculating growth.', type=click.Path(exists=True), required=False)
@click.option('prev5_path', '--prev5', '-g5', help='Path to files from 5 years ago for calculating growth.', type=click.Path(exists=True), required=False)
@click.option('-d', '--debug', is_flag=True)
def main(file_path, year, output_path, prev_path, prev5_path, debug):
    step = 0
    
    if debug:
        step+=1; print; print '''STEP {0}: \nImport file to pandas dataframe'''.format(step)
    rais_df = to_df(file_path, False, debug)

    if debug:
        step+=1; print; print '''STEP {0}: \nReplace vals with DB IDs'''.format(step)
    rais_df = replace_vals(rais_df, {}, debug)

    if debug:
        step+=1; print; print '''STEP {0}: \nAggregate'''.format(step)
    ybio = aggregate(rais_df)

    if debug:
        step+=1; print; print '''STEP {0}: \nShard'''.format(step)
    [yb, yi, yo, ybi, ybo, yio, ybio] = shard(ybio)
    
    if debug:
        step+=1; print; print '''STEP {0}: \nRequired'''.format(step)
    ybio = required(ybio, ybi, yi, year)

    if debug:
        step+=1; print; print '''STEP {0}: \nImportance'''.format(step)
    yio = importance(ybio, ybi, yio, yo, year)

    if debug:
        step+=1; print; print '''STEP {0}: \nDiversity'''.format(step)
    yb = calc_diversity(ybi, yb, "bra_id", "cnae_id", year)
    yb = calc_diversity(ybo, yb, "bra_id", "cbo_id", year)
    yi = calc_diversity(ybi, yi, "cnae_id", "bra_id", year)
    yi = calc_diversity(yio, yi, "cnae_id", "cbo_id", year)
    yo = calc_diversity(ybo, yo, "cbo_id", "bra_id", year)
    yo = calc_diversity(yio, yo, "cbo_id", "cnae_id", year)

    if debug:
        step+=1; print; print '''STEP {0}: \nCalculate RCA, diversity and opportunity gain aka RDO'''.format(step)
    ybi = rdo(ybi, yi, year)
    
    tables = {"yb": yb, "yi": yi, "yo": yo, "ybi": ybi, "ybio": ybio, "ybo": ybo, "yio": yio}

    for table_name, table_data in tables.items():

        table_data = add_column_length(table_name, table_data)
        print table_data.head()
        
    if prev_path:
        if debug:
            step+=1; print; print '''STEP {0}: \nCalculate 1 year growth'''.format(step)
        for t_name, t in tables.items():
            prev_file = os.path.join(prev_path, "{0}.tsv.bz2".format(t_name))
            t_prev = to_df(prev_file, t_name)
            t_prev = t_prev.reset_index(level="year")
            t_prev["year"] = year
            t_prev = t_prev.set_index("year", append=True)
            t_prev = t_prev.reorder_levels(["year"] + list(t_prev.index.names)[:-1])
            
            t = calc_growth(t, t_prev)
            
            if prev5_path:
                if debug:
                    step+=1; print; print '''STEP {0}: \nCalculate 5 year growth'''.format(step)
                prev_file = os.path.join(prev5_path, "{0}.tsv.bz2".format(t_name))
                t_prev = to_df(prev_file, t_name)
                t_prev = t_prev.reset_index(level="year")
                t_prev["year"] = year
                t_prev = t_prev.set_index("year", append=True)
                t_prev = t_prev.reorder_levels(["year"] + list(t_prev.index.names)[:-1])
                
                t_prev = to_df(prev_file, t_name)
                t = calc_growth(t, t_prev, 5)
    
    
    if debug:
        print; print '''FINAL STEP: \nSave files to output path'''
    for t_name, t in tables.items():
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        new_file_path = os.path.abspath(os.path.join(output_path, "{0}.tsv.bz2".format(t_name)))
        t.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.2f")
    

if __name__ == "__main__":
    start = time.time()

    main()
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;