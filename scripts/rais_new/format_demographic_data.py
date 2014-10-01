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
    python scripts/rais_new/format_raw_data.py data/rais/Rais2002.csv.bz2 -y 2002 -o data/rais/2002 -d

"""

''' Import statements '''
import os, sys, time, bz2, click
import pandas as pd
import pandas.io.sql as sql
import numpy as np

from _to_df import to_df
from _aggregate import aggregate, aggregate_demographics
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
def main(file_path, year, output_path, prev_path, prev5_path):
    start = time.time()
    step = 0
    
    d = pd.HDFStore(os.path.abspath(os.path.join(output_path,'rais.h5')))

    step+=1; print; print '''STEP {0}: \nImport file to pandas dataframe'''.format(step)
    rais_df = to_df(file_path, False, calc_d_id=True)

    step+=1; print; print '''STEP {0}: \nAggregate with Demographics'''.format(step)
    tables = aggregate_demographics(rais_df)

    for table_name, table_data in tables.items():
        table_data = add_column_length(table_name, table_data)
        # print table_data.head()
        
    if prev_path:
        step+=1; print; print '''STEP {0}: \nCalculate 1 year growth'''.format(step)
        for t_name, t in tables.items():
            prev_file = os.path.join(prev_path, "{0}.tsv.bz2".format(t_name))
            t_prev = to_df(prev_file, t_name, calc_d_id=True)
            t_prev = t_prev.reset_index(level="year")
            t_prev["year"] = int(year)
            t_prev = t_prev.set_index("year", append=True)
            t_prev = t_prev.reorder_levels(["year"] + list(t_prev.index.names)[:-1])
            
            tables[t_name] = calc_growth(t, t_prev)

            print tables[t_name].head()
            # sys.exit()
            
            if prev5_path:
                step+=1; print; print '''STEP {0}: \nCalculate 5 year growth'''.format(step)
                prev_file = os.path.join(prev5_path, "{0}.tsv.bz2".format(t_name))
                t_prev = to_df(prev_file, t_name, calc_d_id=True)
                t_prev = t_prev.reset_index(level="year")
                t_prev["year"] = int(year)
                t_prev = t_prev.set_index("year", append=True)
                t_prev = t_prev.reorder_levels(["year"] + list(t_prev.index.names)[:-1])
                
                # t_prev = to_df(prev_file, t_name)
                tables[t_name] = calc_growth(t, t_prev, 5)
    
    
    print; print '''FINAL STEP: \nSave files to output path'''
    for t_name, t in tables.items():
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        new_file_path = os.path.abspath(os.path.join(output_path, "{0}.tsv.bz2".format(t_name)))
        t.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.2f")
    
    print("--- %s minutes ---" % str((time.time() - start)/60))

if __name__ == "__main__":
    start = time.time()

    main()
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;