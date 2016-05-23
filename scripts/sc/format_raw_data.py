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
import os
import sys
import time
import bz2
import click
import pandas as pd
import pandas.io.sql as sql
import numpy as np

from _to_df import to_df
from _replace_vals import replace_vals
from _aggregate import aggregate
from _shard import shard
from _calc_rca import calc_rca
from _column_lengths import add_column_length


def pre_check():
    failed = []
    for env_var in ["DATAVIVA_DB_USER", "DATAVIVA_DB_PW", "DATAVIVA_DB_NAME"]:
        if os.environ.get(env_var) is None:
            failed.append(env_var)
    if len(failed):
        sys.exit("The following environment variables need to be set: {0}".format(", ".join(failed)))


@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main(file_path, year, output_path):
    pre_check()
    output_path = os.path.join(output_path)

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    print "Output Path=", output_path
    d = pd.HDFStore(os.path.abspath(os.path.join(output_path, 'sc_data.h5')))
    print
    print '''STEP 1: \nImport file to pandas dataframe'''

    hdf_filepath = output_path + "/store_df.h5"

    print "LOOKING for HDF file at location ", hdf_filepath

    if os.path.exists(hdf_filepath):
        print "READING HDF"
        df = pd.read_hdf(hdf_filepath, 'table')
    else:
        print "No HDF file. Need to create DF"
        df = to_df(file_path, False)
        print "SAVING HDF to", hdf_filepath
        df.to_hdf(hdf_filepath, 'table')

    pk_lookup = {"y": "year", "b": "bra_id", "c": "course_sc_id", "s": "school_id"}

    tables_list = ["yb", "yc", "ys", "ybs", "ybc", "ysc", "ybsc"]

    for table_name in tables_list:
        pk = [pk_lookup[l] for l in table_name]

        print '''\nAggregating {0}'''.format(table_name)
        tbl = aggregate(table_name, pk, df)

        if "c" in table_name:
            pk2 = [x for x in pk]
            pk2[pk2.index("course_sc_id")] = df.course_sc_id.str.slice(0, 2)
            tbl_course2 = aggregate(table_name, pk2, df, course_flag=True)

            tbl = pd.concat([tbl, tbl_course2])

        tbl = add_column_length(table_name, tbl)
        # tbl.rename(columns={"student_id": "students"}, inplace=True)
        file_name = table_name + ".tsv.bz2"
        print '''Save {0} to output path'''.format(file_name)
        new_file_path = os.path.abspath(os.path.join(output_path, file_name))
        tbl.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)


if __name__ == "__main__":
    main()
