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
    python scripts/rais_new/format_raw_data.py data/rais/Rais_2002.csv.bz2 -y 2002 -o data/rais/2002

"""

''' Import statements '''
import os, sys, time, bz2, click, fnmatch
import pandas as pd
import pandas.io.sql as sql
import numpy as np

from _to_df import to_df
from _aggregate import aggregate, aggregate_demographics
from _required import required
from _importance import importance
from _calc_diversity import calc_diversity
from _rdo import rdo
from _growth import calc_growth
from _column_lengths import add_column_length
# from _mne import mne

def findFiles (path, filter):
    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, filter):
            yield os.path.join(root, file)

@click.command()
@click.argument('file_path', type=click.Path(), required=False)
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
@click.option('prev_path', '--prev', '-g', help='Path to files from the previous year for calculating growth.', type=click.Path(exists=True), required=False)
@click.option('prev5_path', '--prev5', '-g5', help='Path to files from 5 years ago for calculating growth.', type=click.Path(exists=True), required=False)
@click.option('--demographics/--no-demographics', '-d', default=False)
def main(file_path, year, output_path, prev_path, prev5_path, demographics):
    start = time.time()
    step = 0
    # regions state, meso, micro, planning region, munic
    depths = {
        "bra": [1, 3, 5, 7, 8, 9],
        "cnae": [1, 3, 6],
        "cbo": [1, 4],
        "demo": [1, 4]
    }
    
    if file_path:
        if not os.path.exists(output_path): os.makedirs(output_path)
        d = pd.HDFStore(os.path.join(output_path, 'rais_df_raw.h5'))
        if "rais_df" in d:
            rais_df = d['rais_df']
        else:
            step+=1; print; print '''STEP {0}: \nImport file to pandas dataframe'''.format(step)
            rais_df = to_df(file_path, False)
            try:
                d['rais_df'] = rais_df
            except OverflowError:
                print "WARNING: Unable to save dataframe, Overflow Error."
        d.close()
        # rais_df = to_df(file_path, False)
    
        step+=1; print; print '''STEP {0}: \nAggregate'''.format(step)
        tables = aggregate(rais_df, depths, demographics)
    
        if not demographics:
            step+=1; print; print 'STEP {0}: \nImportance'.format(step)
            tables["yio"] = importance(tables["ybio"], tables["ybi"], tables["yio"], tables["yo"], year, depths)

            step+=1; print; print 'STEP {0}: \nDiversity'.format(step)
            tables["yb"] = calc_diversity(tables["ybi"], tables["yb"], "bra_id", "cnae_id", year, depths)
            tables["yb"] = calc_diversity(tables["ybo"], tables["yb"], "bra_id", "cbo_id", year, depths)
            tables["yi"] = calc_diversity(tables["ybi"], tables["yi"], "cnae_id", "bra_id", year, depths)
            tables["yi"] = calc_diversity(tables["yio"], tables["yi"], "cnae_id", "cbo_id", year, depths)
            tables["yo"] = calc_diversity(tables["ybo"], tables["yo"], "cbo_id", "bra_id", year, depths)
            tables["yo"] = calc_diversity(tables["yio"], tables["yo"], "cbo_id", "cnae_id", year, depths)

            step+=1; print; print 'STEP {0}: \nCalculate RCA, diversity and opportunity gain aka RDO'.format(step)
            tables["ybi"] = rdo(tables["ybi"], tables["yi"], year, depths)

        for table_name, table_data in tables.items():
            table_data = add_column_length(table_name, table_data)
    
        print; print '''FINAL STEP: \nSave files to output path'''
        for t_name, t in tables.items():
            new_file_path = os.path.abspath(os.path.join(output_path, "{0}.tsv.bz2".format(t_name)))
            t.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.2f")
    
    if prev_path:
        for path, years_ago in ((prev_path, 1), (prev5_path, 5)):
            print years_ago, "year growth"
            if not path: continue
            for current_year_file_path in findFiles(output_path, '*.tsv.bz2'):
                if "growth" in current_year_file_path: continue
                current_year_file_name = os.path.basename(current_year_file_path)
                prev_year_file_path = os.path.join(path, current_year_file_name)
                if not os.path.exists(prev_year_file_path):
                    print "Unable to find", current_year_file_name, "for previous year."
                    continue
                tbl_name, tbl_w_growth = calc_growth(current_year_file_path, prev_year_file_path, year, years_ago)
                print tbl_name
                years_ago_str = "" if years_ago == 1 else "_5"
                new_file_path = os.path.abspath(os.path.join(output_path, "{0}_growth{1}.tsv.bz2".format(tbl_name, years_ago_str)))
                tbl_w_growth.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.3f")

    
    print("--- %s minutes ---" % str((time.time() - start)/60))

if __name__ == "__main__":
    start = time.time()

    main()
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;
