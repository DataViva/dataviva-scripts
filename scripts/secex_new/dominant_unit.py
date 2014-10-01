# -*- coding: utf-8 -*-
"""
    
    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python scripts/secex_new/dominant_unit.py \
    data/secex/export/MDIC_2002.rar \
    data/secex/import/MDIC_2002.rar \
    -y 2002

"""

''' Import statements '''
import os, sys, time, bz2, click, MySQLdb, rarfile
import pandas as pd
import numpy as np

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA2_DB_USER"], 
                        passwd=os.environ["DATAVIVA2_DB_PW"], 
                        db=os.environ["DATAVIVA2_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

cursor.execute("select substr(id, 3), id from attrs_hs where substr(id, 3) != '' and length(id) = 6;")
hs_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
hs_lookup["9991"] = "229999"
hs_lookup["9992"] = "229999"
hs_lookup["9998"] = "229999"
hs_lookup["9997"] = "229999"

unit_lookup = {"10":"Liquid Kilogram",
    "19":"carat",
    "17":"liter",
    "23":"billions of international units",
    "12":"thousands",
    "21":"metric ton liquid",
    "24":"gross kilogram",
    "15":"square meter",
    "22":"liquid gram",
    "11":"number (unit)",
    "13":"pairs",
    "14":"meter",
    "18":"thousand kw hour",
    "16":"cubic meter",
    "20":"dozen",
    "ZZ":"not declared"}

def hs_replace(raw):
    try: return hs_lookup[str(raw).strip()]
    except: return raw

def unit_replace(raw):
    try: return unit_lookup[str(raw).strip()]
    except: return raw

@click.command()
@click.argument('export_file_path', type=click.Path(exists=True))
@click.argument('import_file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
def main(export_file_path, import_file_path, year):
    step = 0
    df = pd.DataFrame()
    
    step += 1; print '''\nSTEP {0}: \nImport file to pandas dataframe'''.format(step)
    for file_path in [export_file_path, import_file_path]:
        rar_file = rarfile.RarFile(export_file_path)
        file_name = os.path.basename(export_file_path)
        file_path_no_ext, file_ext = os.path.splitext(file_name)
        input_file = rarfile.RarFile.open(rar_file, file_path_no_ext+".csv")
    
        cols = ["year", "month", "wld_id", "state_id", "customs", "bra_id", "unit", \
                "val_unit", "val_kg", "val_usd", "hs_id"]
        delim = "|"
        # coerce_cols = {"hs_id":hs_replace, "unit":unit_replace}
        coerce_cols = {"hs_id":hs_replace}
        secex_df = pd.read_csv(input_file, header=0, sep=delim, converters=coerce_cols, names=cols)    
        secex_df = secex_df[["hs_id", "val_usd", "unit"]]
        df = pd.concat([df, secex_df])

    secex_df = secex_df.groupby(["hs_id", "unit"]).count()
    
    hs_unit = {}
    for (hs, unit), row in secex_df.iterrows():
        val = row['val_usd']
        if hs in hs_unit:
            if val > hs_unit[hs]:
                hs_unit[hs] = unit
        else:
            hs_unit[hs] = unit
    
    for hs, unit in hs_unit.iteritems():
        cursor.execute("UPDATE `attrs_hs` SET `unit` = %s WHERE `id` = %s;", [unit, hs])

if __name__ == "__main__":
    start = time.time()

    main()
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;