# -*- coding: utf-8 -*-
"""
    Format SECEX data for DB entry
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Columns:
    0:  Year
    1:  Enroll_ID
    2:  Studant_ID
    3:  Age
    4:  Gender
    5:  Color
    6:  Education_Mode
    7:  Education_Level
    8:  Education_Level_New
    9:  Education
    10: Class_ID
    11: Course_ID
    12: School_ID
    13: Municipality
    14: Location
    15: Adm_Dependency
    
    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python scripts/edu/format_raw_data.py data/edu/School_census_2007.csv.bz2 -y 2007 -o data/edu/

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
from _calc_rca import calc_rca

def pre_check():
    failed = []
    for env_var in ["DATAVIVA2_DB_USER", "DATAVIVA2_DB_PW", "DATAVIVA2_DB_NAME"]:
        if os.environ.get(env_var) is None:
            failed.append(env_var)
    if len(failed):
        sys.exit("The following environment variables need to be set: {0}".format(", ".join(failed)))

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main(file_path, year, output_path):
    step = 0
    pre_check()
    
    d = pd.HDFStore(os.path.abspath(os.path.join(output_path,'edu_data.h5')))
    if "ybsc" in d:
        ybsc = d["ybsc"]
    else:
        step+=1; print; print '''STEP {0}: \nImport file to pandas dataframe'''.format(step)
        df = to_df(file_path, False)
    
        step+=1; print; print '''STEP {0}: \nReplace vals with DB IDs'''.format(step)
        df = replace_vals(df, {})
    
        step+=1; print; print '''STEP {0}: \nAggregate'''.format(step)
        ybsc = aggregate(df)
        
        d["ybsc"] = ybsc
    
    step+=1; print; print '''STEP {0}: \nShard'''.format(step)
    [yb, ys, yc, ybs, ybc, ysc, ybsc] = shard(ybsc)
    
    print yb.head()
    sys.exit()
    
    step+=1; print; print '''STEP {0}: \nCalc RCA'''.format(step)
    ybc = calc_rca(ybc, year)

    tables = {"yb": yb, "ys": ys, "yc": yc, "ybs": ybs, "ybc": ybc, "ysc": ysc, "ybsc": ybsc}
    
    print; print '''FINAL STEP: \nSave files to output path'''
    for t_name, t in tables.items():
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        new_file_path = os.path.abspath(os.path.join(output_path, "{0}.tsv.bz2".format(t_name)))
        t.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)

if __name__ == "__main__":
    main()