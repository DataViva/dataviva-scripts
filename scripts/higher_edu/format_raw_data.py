# -*- coding: utf-8 -*-
"""
    Format Higher ED data for DB entry
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Columns:
    0  Academic_organization
    1  Municipality
    2  ID_university
    3  Year (2000 - 2012)
    4  Adm_category
    5  ID_course
    6  Name_course
    7  Modality
    8  Level
    9  Openings
    10 Enrolled
    11 Graduates
    12 Entrants
    13 Degree
    
    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python scripts/higher_edu/format_raw_data.py data/higher_edu/undergraduate.csv -o -y data/higher_edu/

"""

''' Import statements '''
import os, sys, time, bz2, click
import pandas as pd
import pandas.io.sql as sql
import numpy as np

from _to_df import to_df
from _replace_vals import replace_vals
# from _aggregate import aggregate
# from _shard import shard
from _calc_rca import calc_rca

def pre_check():
    failed = []
    for env_var in ["DATAVIVA2_DB_USER", "DATAVIVA2_DB_PW", "DATAVIVA2_DB_NAME"]:
        if os.environ.get(env_var) is None:
            failed.append(env_var)
    if len(failed):
        sys.exit("The following environment variables need to be set: {0}".format(", ".join(failed)))

def get_years(year):
    possible_years = range(2000, 2013)
    if year == "all":
        return possible_years
    else:
        try:
            year = int(year)
        except:
            sys.exit("Invalid year provided.")
        if year not in possible_years:
            sys.exit("Invalid year provided. Must be between {0} and {1}.".format(possible_years[0], possible_years[-1]))
        return [year]

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True, default='all')
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main(file_path, year, output_path):
    pre_check()
    years = get_years(year)
    
    for y in years:
        print "\nYEAR: {0}\n".format(y)
        this_output_path = os.path.join(output_path, str(y))
        if not os.path.exists(this_output_path): os.makedirs(this_output_path)
        
        step = 0
        step+=1; print '''STEP {0}: Import file to pandas dataframe'''.format(step)
        df = to_df(file_path, y)

        step+=1; print '''STEP {0}: Replace vals with DB IDs'''.format(step)
        df = replace_vals(df, {})
    
        df = df.rename(columns={"munic":"bra_id"})
        ybuc = df.groupby(["year", "bra_id", "university_id", "course_id"]).sum()
        
        step+=1; print '''STEP {0}: Calculating RCAs'''.format(step)
        ybc = calc_rca(ybuc, y)
    
        tables = {"ybuc":ybuc, "ybc":ybc}
        print '''FINAL STEP: Save files to output path'''
        for t_name, t in tables.items():
            new_file_path = os.path.abspath(os.path.join(this_output_path, "{0}.tsv.bz2".format(t_name)))
            t.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)

if __name__ == "__main__":
    main()