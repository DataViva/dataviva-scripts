# -*- coding: utf-8 -*-
import os
import sys
import bz2
import click
import pandas as pd

from _to_df import to_df
from _aggregate import aggregate
from _column_lengths import add_column_length

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


def pre_check():
    failed = []
    for env_var in ["DATAVIVA_DB_USER", "DATAVIVA_DB_PW", "DATAVIVA_DB_NAME"]:
        if os.environ.get(env_var) is None:
            failed.append(env_var)
    if len(failed):
        sys.exit("The following environment variables need to be set: {0}".format(
            ", ".join(failed)))


@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.',
              type=click.Path(), required=True, prompt="Output path")
@click.option('prev_path', '--prev', '-g', help='Path to files from the previous year for calculating growth.',
              type=click.Path(exists=True), required=False)
@click.option('prev5_path', '--prev5', '-g5', help='Path to files from 5 years ago for calculating growth.',
              type=click.Path(exists=True), required=False)
def main(file_path, year, output_path, prev_path, prev5_path):
    print "\nSC YEAR: {0}\n".format(year)
    pre_check()
    output_path = os.path.join(output_path, str(year))

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    hdf_store = pd.HDFStore(os.path.abspath(os.path.join(output_path, 'sc_data.h5')))

    print '''\nImport file to pandas dataframe'''

    if "sc_df" in hdf_store:
        sc_df = hdf_store['sc_df']
    else:
        sc_df = to_df(file_path, False)
        try:
            hdf_store['sc_df'] = sc_df
        except OverflowError:
            print "WARNING: Unable to save dataframe, Overflow Error."
            hdf_store.close()
            os.remove(os.path.join(output_path, 'sc_data.h5'))

    tables_list = ["yb", "yc", "ys", "ybs", "ybc", "ysc", "ybsc"]
    index_lookup = {"y": "year", "b": "bra_id", "c": "course_sc_id", "s": "school_id"}

    for table_name in tables_list:
        indexes = [index_lookup[l] for l in table_name]

        print '''\nAggregating {0}'''.format(table_name)
        aggregated_df = aggregate(indexes, sc_df)

        print '''Adding length column to {0}'''.format(table_name)
        aggregated_df = add_column_length(table_name, aggregated_df)

        file_name = table_name + ".tsv.bz2"
        print '''Save {0} to output path'''.format(file_name)
        new_file_path = os.path.abspath(os.path.join(output_path, file_name))
        aggregated_df.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)


if __name__ == "__main__":
    main()
