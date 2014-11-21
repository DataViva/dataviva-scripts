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
from _aggregate import aggregate
from _column_lengths import add_column_length

from _calc_rca import calc_rca
# from _aggregate import aggregate
# from _shard import shard
# from _demographic_calc import compute_demographics

def pre_check():
    failed = []
    for env_var in ["DATAVIVA2_DB_USER", "DATAVIVA2_DB_PW", "DATAVIVA2_DB_NAME"]:
        if os.environ.get(env_var) is None:
            failed.append(env_var)
    if len(failed):
        sys.exit("The following environment variables need to be set: {0}".format(", ".join(failed)))

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True, default='all')
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main(file_path, year, output_path):
    pre_check()
    output_path = os.path.join(output_path, str(year))

    print "\nYEAR: {0}\n".format(year)
    this_output_path = os.path.join(output_path)
    if not os.path.exists(this_output_path): os.makedirs(this_output_path)
    
    step = 0
    step+=1; print '''STEP {0}: Import file to pandas dataframe'''.format(step)
    df = to_df(file_path, year)
    
    tables_list = ["yb", "ybd", "yd", "ybc", "yc", "ybu"]
    pk_lookup = {"y": "year", "d": "d_id", "b": "bra_id", "c": "course_hedu_id", "u": "university_id"}

    ybuc = None

    for table_name in tables_list:
        pk = [pk_lookup[l] for l in table_name]
        print "working on", table_name
        
        dems = ['gender', 'ethnicity', 'school_type'] if "d" in table_name else ['']
        
        for dem in dems:
            print '''\nSTEP 2: Aggregate {0}'''.format(dem)
            tbl = aggregate(pk, df, dem)
            
            if "c" in table_name:
                pk2 = [x for x in pk]
                pk2[pk2.index("course_hedu_id")] = df.course_hedu_id.str.slice(0, 2)
                # df2.course_hedu_id = df.course_hedu_id.str.slice(0, 2)
                tbl_course2 = aggregate(pk2, df, dem)

                tbl = pd.concat([tbl, tbl_course2])
            
            tbl = add_column_length(table_name, tbl)
            tbl.rename(columns={"student_id": "students"}, inplace=True)   
            if table_name == "yb":
                tbl.rename(columns={"university_id": "num_universities"}, inplace=True)   
            if table_name == "ybuc":
                print tbl.head()
                ybuc = tbl
            file_name = table_name + "_" + dem + ".tsv.bz2" if "d" in table_name else table_name + ".tsv.bz2"
            print '''Save {0} to output path'''.format(file_name)
            new_file_path = os.path.abspath(os.path.join(output_path, file_name))
            tbl.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)

        # if "c" in table_name:
        #     print '''\nSTEP 3: Aggregate {0}'''
        #     tbl = aggregate(pk, df, '', 2)
        #     tbl = add_column_length(table_name, tbl)
        #     # print tbl.reset_index().course_hedu_id.nunique()
        #     file_name = table_name + "_cid2.tsv.bz2"
        #     print '''Save {0} to output path'''.format(file_name)
        #     new_file_path = os.path.abspath(os.path.join(output_path, file_name))
        #     tbl.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)        
    
    if ybuc is not None:
        step+=1; print '''STEP {0}: Calculating RCAs'''.format(step)
        ybc = calc_rca(ybuc, year)
        new_file_path = os.path.abspath(os.path.join(output_path, "ybc_rca.tsv.bz2"))
        ybc.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)
        print "writing", new_file_path
    

if __name__ == "__main__":
    main()
