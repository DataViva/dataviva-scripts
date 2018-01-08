# -*- coding: utf-8 -*-
import os
import sys
import bz2
import click
import time
import pandas as pd
import sendgrid

from _to_df import to_df
from _aggregate import aggregate
from _column_lengths import add_column_length
from _growth import calc_growth

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


def open_prev_df(prev_path, table_name, year, indexes):
    prev_file = os.path.join(prev_path, "{0}.tsv.bz2".format(table_name))
    previous_df = to_df(prev_file, indexes)
    previous_df = previous_df.reset_index(level="year")
    previous_df["year"] = int(year)
    previous_df = previous_df.set_index("year", append=True)
    previous_df = previous_df.reorder_levels(
        ["year"] + list(previous_df.index.names)[:-1])
    return previous_df


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
    start = time.time()
    pre_check()
    output_path = os.path.join(output_path, str(year))

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    hdf_store = pd.HDFStore(
        os.path.abspath(os.path.join(output_path, 'sc_data.h5')))

    print '''\nImport file to pandas dataframe'''

    if "sc_df" in hdf_store:
        sc_df = hdf_store['sc_df']
    else:
        sc_df = to_df(file_path)
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

        print '''Renaming {0} columns'''.format(table_name)
        aggregated_df.rename(columns={"enroll_id": "enrolled"}, inplace=True)
        aggregated_df.rename(columns={"class_id": "classes"}, inplace=True)
        if 's' not in table_name:
            aggregated_df.rename(columns={"school_id": "num_schools"}, inplace=True)

        if prev_path:
            print '''\nCalculating {0} 1 year growth'''.format(table_name)
            previous_df = open_prev_df(prev_path, table_name, year, indexes)
            aggregated_df = calc_growth(aggregated_df, previous_df, ['enrolled'])

        if prev5_path:
            print '''\nCalculating {0} 5 year growth'''.format(table_name)
            previous_df = open_prev_df(prev5_path, table_name, year, indexes)
            aggregated_df = calc_growth(aggregated_df, previous_df, ['enrolled'], 5)

        file_name = table_name + ".tsv.bz2"
        print '''\nSave {0} to output path'''.format(file_name)
        new_file_path = os.path.abspath(os.path.join(output_path, file_name))
        aggregated_df.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)

    time_elapsed = "%s minutes" % str((time.time() - start) / 60)

    print '''\nTotal time %s''' % time_elapsed
    print '''\nSending alert e-mail'''

    client = sendgrid.SendGridClient(os.environ['SENDGRID_API_KEY'])
    message = sendgrid.Mail()

    message.add_to(os.environ.get('ADMIN_EMAIL', 'contato@dataviva.info'))
    message.set_from("calc-server@dataviva.info")
    message.set_subject("Scholar census %s ready!" % year)
    message.set_html("Your calculation took %s, please check out the output at the calc-server" % time_elapsed)

    client.send(message)

if __name__ == "__main__":
    main()
