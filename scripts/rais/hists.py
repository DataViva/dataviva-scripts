# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import sys
import math
import MySQLdb
import click
import time
import pandas as pd
import sendgrid
from _to_df import to_df

'''
    Usage:
    python -u scripts/rais/hists.py -y 2014 -o data/rais -a cbo > load_rais_hists_cbo.log &
'''

latest_year = 2014

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"],
                     passwd=os.environ["DATAVIVA_DB_PW"],
                     db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

depths_lookup = {
    "bra": [1, 3, 5, 7, 9],
    "cbo": [1, 4],
    "cnae": [1, 3, 6]
}
table_lookup = {
    "bra": "rais_yb",
    "cbo": "rais_yo",
    "cnae": "rais_yi"
}


def roundup(x):
    return int(math.ceil(x / 100.0)) * 100


def rounddown(x):
    return int(math.floor(x / 100.0)) * 100


def hist(id, df, min_val, max_val, bin_size, attr_type, year):
    df['wage_clipped'] = df['wage'].clip(lower=min_val, upper=max_val)
    bins = range(min_val, max_val + bin_size, bin_size)
    df['bin'] = pd.cut(df.wage_clipped, bins)
    # hist = str(df.bin.value_counts().to_dict()).replace(' ', '').replace(']', '').replace('(', '')
    hist_0s = {"{},{}".format(b, bins[i + 1]): 0 for i, b in enumerate(bins[:-1])}
    hist = {k.replace(' ', '').replace(']', '').replace(
        '(', ''): v for k, v in df.bin.value_counts().to_dict().items()}
    full_hist = hist_0s.copy()
    full_hist.update(hist)
    full_hist = str(full_hist).replace(' ', '')
    table = table_lookup[attr_type]
    cursor.execute("update {} set hist=%s where year=%s and {}_id=%s".format(table, attr_type), (full_hist, year, id))


@click.command()
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.',
              type=click.Path(), required=True, prompt="Output path")
@click.option('--attr_type', '-a', type=click.Choice(['bra', 'cbo', 'cnae']), required=True, prompt="Attr Type")
def main(year, output_path, attr_type):

    if "-" in year:
        years = range(int(year.split('-')[0]), int(year.split('-')[1]) + 1)
    else:
        years = [int(year)]
    print("years:", str(years))

    start = time.time()
    for year in years:
        d = pd.HDFStore(os.path.join(output_path, str(year), 'rais_df_raw.h5'))
        if "rais_df" in d:
            rais_df = d['rais_df']
        else:
            file_path = os.path.join(output_path, 'Rais_{}.csv'.format(year))
            rais_df = to_df(file_path)
            d['rais_df'] = rais_df

        hist_bins = pd.HDFStore(os.path.join(output_path, '{}_hist_bins.h5'.format(attr_type)))

        for depth in depths_lookup[attr_type]:
            print("\n{} depth: {}\n".format(attr_type, depth))

            this_depth_df = rais_df.copy()
            this_depth_df['{}_id'.format(attr_type)] = this_depth_df['{}_id'.format(attr_type)].str.slice(0, depth)

            # uniqs = ['1112', '8401', '8202', '7842', '7621']:
            uniqs = this_depth_df["{}_id".format(attr_type)].unique()
            for i, id in enumerate(uniqs):
                this_id_df = this_depth_df[this_depth_df['{}_id'.format(attr_type)] == id]
                if len(this_id_df.index) < 10:
                    print("\nNot enough occurences for histogram")
                    continue
                print("********* {}: {} ({}/{}) *********".format(year, id, i + 1, len(uniqs)), end='\r')
                sys.stdout.flush()

                if int(year) == latest_year:
                    wage = this_id_df["wage"]
                    wmin = rounddown(wage.mean() - (wage.std() * 2))
                    wmin = 0 if wmin < 0 else wmin
                    wmax = rounddown(wage.mean() + (wage.std() * 2))
                    wrange = wmax - wmin
                    # print wrange
                    bin_size = 100
                    if wrange > 3000:
                        bin_size = 200
                    if wrange > 5000:
                        bin_size = 500
                    if wrange > 10000:
                        bin_size = 1000

                    ''' !!! exception for regions (all need to have same bins!) !!! '''
                    if attr_type == "bra" and depth == 1:
                        bin_size = 200
                        wmin = 0
                        wmax = 5200

                    hist_bins["{}_{}".format(attr_type, id)] = pd.Series([wmin, wmax, bin_size])
                else:
                    if "{}_{}".format(attr_type, id) in hist_bins:
                        wmin, wmax, bin_size = hist_bins["{}_{}".format(attr_type, id)]
                    else:
                        continue

                hist(id, this_id_df, wmin, wmax, bin_size, attr_type, year)

        d.close()
        hist_bins.close()
    time_elapsed = "%s minutes" % str((time.time() - start) / 60)

    print('''\nTotal time %s''' % time_elapsed)
    print('''\nSending alert e-mail''')

    client = sendgrid.SendGridClient(os.environ['SENDGRID_API_KEY'])
    message = sendgrid.Mail()

    message.add_to(os.environ.get('ADMIN_EMAIL', 'contato@dataviva.info'))
    message.set_from("calc-server@dataviva.info")
    message.set_subject("Rais histogram for %s ready!" % year)
    message.set_html("Your calculation took %s, please check out the output at the calc-server" % time_elapsed)

    client.send(message)
if __name__ == "__main__":
    main()
