# -*- coding: utf-8 -*-
from __future__ import print_function
import os, sys, click, MySQLdb, csv
import pandas as pd

'''
USAGE:
python scripts/attrs/import_proximity.py -y 2014 -a wld -i bra -o data/attr
'''

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
cursor = db.cursor()

attr_types = ['bra', 'hs', 'wld', 'cnae', 'cbo', 'course_sc', 'course_hedu', 'university']

table_lookup = {
    "bra": "attrs_ybb",
    "cbo": "attrs_yoo",
    "cnae": "attrs_yii",
    "hs": "attrs_ypp",
    "university": "attrs_yuu",
    "wld": "attrs_yww"
}

@click.command()
@click.option('-a', '--attr_type', type=click.Choice(attr_types), required=True, prompt="Attr Type")
@click.option('-i', '--i_attr', type=click.Choice(attr_types), required=True, prompt='Intermediate Attr')
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def import_prox(attr_type, i_attr, year, output_path):

    if "-" in year:
        years = range(int(year.split('-')[0]), int(year.split('-')[1])+1)
    else:
        years = [int(year)]
    print("years:", str(years))
    
    tbl = table_lookup[attr_type]
    col = "prox_{}".format(i_attr)
    
    for year in years:
        
        file_path = os.path.join(output_path, str(year), '{}_{}_proximity.tsv'.format(attr_type, i_attr))
        prox_df = pd.read_csv(file_path, sep="\t", index_col=["bra_id", "bra_id.1"])
        prox_df = prox_df.drop("year", axis=1)
        all_ids = prox_df.index.levels[0]
        
        for i, this_id in enumerate(all_ids):
            print("********* {}:{} ({}/{}) *********".format(year, this_id, i, len(all_ids)), end='\r')
            sys.stdout.flush()
            this_id_prox = prox_df.ix[this_id]["{}_proximity".format(i_attr)].order(ascending=False)[1:101]
            for this_id_target, prox in this_id_prox.iteritems():
                cursor.execute("""
                    INSERT INTO {0}(year, {1}_id, {1}_id_target, {2})
                    VALUES(%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    {2}=VALUES({2})""".format(tbl, attr_type, col), [year, this_id, this_id_target, prox])
        print('')
        db.commit()
        
        # with open(file_path) as f:
        #     row_count = sum(1 for row in f)
        #     f.seek(0)
        #     csv_data = csv.reader(f, delimiter="\t")
        #     headers = csv_data.next()
        #     for i, row in enumerate(csv_data):
        #
        #         this_id, this_id_target, year, prox = row
        #         ''' dont want to include proximities to self '''
        #         if this_id == this_id_target: continue
        #         if not prox: continue
        #         prox = float(prox)
        #
        #         print("********* {}:{} ({}/{}) *********".format(year, this_id, i, row_count), end='\r')
        #         sys.stdout.flush()
        #
        #         cursor.execute("""
        #             INSERT INTO {0}(year, {1}_id, {1}_id_target, {2})
        #             VALUES(%s, %s, %s, %s)
        #             ON DUPLICATE KEY UPDATE
        #             {2}=VALUES({2})""".format(tbl, attr_type, col), [year, this_id, this_id_target, prox])
        #     #close the connection to the database.
        #     print('')
        #     db.commit()
        
    cursor.close()

if __name__ == "__main__":
    import_prox()
