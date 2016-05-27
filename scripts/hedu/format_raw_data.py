# -*- coding: utf-8 -*-
import os
import sys
import bz2
import click
import pandas as pd

from _to_df import to_df
from _aggregate import aggregate
from _column_lengths import add_column_length
from _growth import calc_growth
from _calc_rca import calc_rca

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
    python scripts/hedu/format_raw_data.py data/hedu/raw/Undergraduate_census_2014_microdata.csv -y 2014 -o data/hedu
    -g data/hedu/2013 -g5 data/hedu/2009

"""


def pre_check():
    failed = []
    for env_var in ["DATAVIVA_DB_USER", "DATAVIVA_DB_PW", "DATAVIVA_DB_NAME"]:
        if os.environ.get(env_var) is None:
            failed.append(env_var)
    if len(failed):
        sys.exit("The following environment variables need to be set: {0}".format(", ".join(failed)))


def open_prev_df(prev_path, table_name, year, indexes):
    prev_file = os.path.join(prev_path, "{0}.tsv.bz2".format(table_name))
    previous_df = to_df(prev_file, year, indexes)
    previous_df = previous_df.reset_index(level="year")
    previous_df["year"] = int(year)
    previous_df = previous_df.set_index("year", append=True)
    previous_df = previous_df.reorder_levels(["year"] + list(previous_df.index.names)[:-1])
    return previous_df


@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True, default='all')
@click.option('output_path', '--output', '-o', help='Path to save files to.',
              type=click.Path(), required=True, prompt="Output path")
@click.option('prev_path', '--prev', '-g', help='Path to files from the previous year for calculating growth.',
              type=click.Path(exists=True), required=False)
@click.option('prev5_path', '--prev5', '-g5', help='Path to files from 5 years ago for calculating growth.',
              type=click.Path(exists=True), required=False)
def main(file_path, year, output_path, prev_path, prev5_path):
    print "\nHEDU YEAR: {0}\n".format(year)
    pre_check()
    output_path = os.path.join(output_path, str(year))

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    hdf_store = pd.HDFStore(os.path.abspath(os.path.join(output_path, 'hedu_data.h5')))

    print '''\nImport file to pandas dataframe'''

    if "hedu_df" in hdf_store:
        hedu_df = hdf_store['hedu_df']
    else:
        hedu_df = to_df(file_path, year)
        try:
            hdf_store['hedu_df'] = hedu_df
        except OverflowError:
            print "WARNING: Unable to save dataframe, Overflow Error."
            hdf_store.close()
            os.remove(os.path.join(output_path, 'hedu_data.h5'))

    tables_list = ["yb", "yu", "yc", "ybc", "ybu", "yuc", "ybuc"]
    index_lookup = {"y": "year", "b": "bra_id", "c": "course_hedu_id", "u": "university_id"}

    for table_name in tables_list:
        indexes = [index_lookup[l] for l in table_name]

        print '''\nAggregating {0}'''.format(table_name)
        aggregated_df = aggregate(indexes, hedu_df)

        print '''Adding length column to {0}'''.format(table_name)
        aggregated_df = add_column_length(table_name, aggregated_df)

        print '''Renaming {0} columns'''.format(table_name)
        aggregated_df.rename(columns={"student_id": "students"}, inplace=True)
        if 'u' not in table_name:
            aggregated_df.rename(columns={"university_id": "num_universities"}, inplace=True)

        if prev_path:
            print '''\nCalculating {0} 1 year growth'''.format(table_name)
            previous_df = open_prev_df(prev_path, table_name, year, indexes)
            aggregated_df = calc_growth(aggregated_df, previous_df, ['enrolled', 'graduates'])

        if prev5_path:
            print '''\nCalculating {0} 5 year growth'''.format(table_name)
            previous_df = open_prev_df(prev5_path, table_name, year, indexes)
            aggregated_df = calc_growth(aggregated_df, previous_df, ['enrolled', 'graduates'], 5)

        if table_name == "ybuc":
            print '''Calculating RCAs'''
            ybc = calc_rca(aggregated_df, year)
            new_file_path = os.path.abspath(os.path.join(output_path, "ybc_rca.tsv.bz2"))
            ybc.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)

        file_name = table_name + ".tsv.bz2"
        print '''Save {0} to output path'''.format(file_name)
        new_file_path = os.path.abspath(os.path.join(output_path, file_name))
        aggregated_df.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)

if __name__ == "__main__":
    main()
