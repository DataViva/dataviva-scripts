# -*- coding: utf-8 -*-
from __future__ import print_function
import os, sys, math, MySQLdb, click, time
import pandas as pd
from scripts.rais._to_df import to_df
from numpy import argsort

'''
    Usage:
    python gini.py -y 2013 -o data/rais/ -a bra -t rais_yb
'''

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

def gini_coeff(x):
    n = len(x)
    s = x.sum()
    r = argsort(argsort(-x)) # calculates zero-based ranks
    return 1 - (2.0 * (r*x).sum() + s)/(n*s)

@click.command()
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
@click.option('--attr_type', '-a', type=click.Choice(['bra','cbo','cnae']), required=True, prompt="Attr Type")
def main(year, output_path, attr_type):

    if "-" in year:
        years = range(int(year.split('-')[0]), int(year.split('-')[1])+1)
    else:
        years = [int(year)]
    print("years:", str(years))

    for year in years:
        start = time.time()
        d = pd.HDFStore(os.path.join(output_path, str(year), 'rais_df_raw.h5'))
        if "rais_df" in d:
            rais_df = d['rais_df']
        else:
            file_path = os.path.join(output_path,'Rais_{}.csv.bz2'.format(year))
            rais_df = to_df(file_path)
            d['rais_df'] = rais_df

        for depth in depths_lookup[attr_type]:
            print("\n{} depth: {}\n".format(attr_type, depth))

            this_depth_df = rais_df.copy()
            this_depth_df['{}_id'.format(attr_type)] = this_depth_df['{}_id'.format(attr_type)].str.slice(0, depth)

            uniqs = this_depth_df["{}_id".format(attr_type)].unique()[::-1]
            for i, id in enumerate(uniqs):
                this_id_df = this_depth_df[this_depth_df['{}_id'.format(attr_type)] == id]
                if len(this_id_df.index) < 10:
                    print("\nNot enough occurences for histogram thus no GINI (need at least 10)")
                    continue
                print("********* {}: {} ({}/{}) *********".format(year, id, i+1, len(uniqs)), end='\r')
                sys.stdout.flush()
                wage = this_id_df["wage"]
                gini = gini_coeff(wage)
                # print(gini)
                table = table_lookup[attr_type]
                cursor.execute("update {} set gini=%s where year=%s and {}_id=%s".format(table, attr_type), (gini, year, id))
                # raw_input('')

        d.close()
        print("\n\n--- %s minutes ---\n\n" % str((time.time() - start)/60))

if __name__ == "__main__":
    main()
