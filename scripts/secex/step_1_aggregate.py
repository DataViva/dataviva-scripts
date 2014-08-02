# -*- coding: utf-8 -*-
"""
    Clean raw SECEX data and output to TSV
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the first step in adding a new year of SECEX data to the 
    database. The script will output 1 bzipped TSV file that can then be 
    consumed by step 2 for creating the disaggregate tables.
    
    How to run this: 
    python -m scripts.secex.step_1_aggregate \
        -y 2000 \
        data/secex/export/MDIC_2000.csv.zip
    
    * you can also pass an optional second argument of the path for the output
      file. This output file should end in .bz2 as the output is always bzipped.

"""


''' Import statements '''
import csv, sys, os, MySQLdb, time, bz2, click
import pandas as pd
from collections import defaultdict
from ..config import DATA_DIR
from ..helpers import d, get_file, format_runtime

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def get_lookup(type):
    if type == "bra":
        cursor.execute("select id_mdic, id from attrs_bra where length(id)=8")
        lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
        lookup["4314548"] = "rs030014"
        lookup["9999999"] = "xx000007"
        lookup["9400000"] = "xx000002"
    if type == "state":
        cursor.execute("select id_mdic, id from attrs_bra where length(id)=2")
        lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
        lookup["94"] = "xx"
        lookup["95"] = "xx"
        lookup["96"] = "xx"
        lookup["97"] = "xx"
        lookup["98"] = "xx"
        lookup["99"] = "xx"
    elif type == "hs":
        #Example: Original number 01010119 will be 010119
        cursor.execute("select id from attrs_hs where length(id)=6")
        lookup = {r[0][2:]:r[0] for r in cursor.fetchall()}
        lookup["9991"] = "229999"
        lookup["9992"] = "229999"
        lookup["9998"] = "229999"
        lookup["9997"] = "229999"
    elif type == "wld":
        cursor.execute("select id_mdic, id from attrs_wld where id_mdic is not null and length(id) = 5;")
        lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
        lookup["695"] = "nakna"
        lookup["423"] = "asmys"
        lookup["152"] = "euchi"
    elif type == "pr":
        cursor.execute("select bra_id, pr_id from attrs_bra_pr")
        lookup = {r[0]:r[1] for r in cursor.fetchall()}
    return lookup

def add(ybpw, munic, isic, occ, val_usd):
    ybpw[munic][isic][occ]["val_usd"] += val_usd
    return ybpw

@click.command()
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('output_file', type=click.File('wb'), required=False)
def aggregate(year, input_file, output_file):
    
    if not output_file:
        dirname = os.path.dirname(input_file)
        year_dir = os.path.abspath(os.path.join(dirname, str(year)))
        if not os.path.exists(year_dir):
            os.makedirs(year_dir)
        output_file = os.path.abspath(os.path.join(year_dir, "ybpw.tsv.bz2"))
    
    click.echo(click.format_filename(input_file))
    input_file = get_file(input_file)

    cols = ["year", "month", "wld", "state", "customs", "munic", "unit", \
            "quantity", "val_kg", "val_usd", "hs"]
    delim = "|"
    secex_df = pd.read_csv(input_file, header=0, sep=delim, converters={"munic":str, "month":str, "hs":str}, names=cols)
    secex_df = secex_df[["year", "month", "state", "munic", "hs", "wld", "val_usd"]]
    # hs = pd.DataFrame(secex_df.hs.unique())
    # hs['len'] = hs[0].apply(lambda x: len(x))
    # print hs.head()
    # hs.to_csv('unique_hs.csv', index=False)
    # sys.exit()

    replacements = [
        {"col":"munic", "lookup":get_lookup("bra")},
        {"col":"state", "lookup":get_lookup("state")},
        {"col":"hs", "lookup":get_lookup("hs")},
        {"col":"wld", "lookup":get_lookup("wld")}
    ]

    missing = None
    for r in replacements:
        print "Replacing {0} IDs".format(r["col"])
        num_rows = secex_df.shape[0]
        secex_df[r["col"]] = secex_df[r["col"]].astype(str).replace(r["lookup"])
        not_in_db = set(secex_df[r["col"]].unique()).difference(set(r["lookup"].values()))
        if not_in_db:
            if missing != None:
                if r["col"] in missing:
                    missing[r["col"]] = missing[r["col"]].union(not_in_db)
                else:
                    missing[r["col"]] = not_in_db
            else:
                print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(r["col"]);
                drop_criterion = secex_df[r["col"]].map(lambda x: x not in not_in_db)
                secex_df = secex_df[drop_criterion]
                print not_in_db; print "{0} rows deleted.".format(num_rows - secex_df.shape[0]); print;
    
    secex_df = secex_df.drop("month", axis=1)
    
    ymbpw = secex_df.groupby(["year", "state", "munic", "hs", "wld"]).sum()
    
    ymbpw_states = ymbpw.groupby(level=["year", "state", "hs", "wld"]).sum()
    ymbpw_states.index.names = ["year", "bra_id", "hs", "wld"]
    
    ymbpw_munics = ymbpw.reset_index()
    ymbpw_munics = ymbpw_munics.drop("state", axis=1)
    ymbpw_munics = ymbpw_munics.groupby(["year", "munic", "hs", "wld"]).sum()
    ymbpw_munics.index.names = ["year", "bra_id", "hs", "wld"]
    
    ymbpw_meso = ymbpw_munics.reset_index()
    ymbpw_meso["bra_id"] = ymbpw_meso["bra_id"].apply(lambda x: x[:4])
    ymbpw_meso = ymbpw_meso.groupby(["year", "bra_id", "hs", "wld"]).sum()
    
    ymbpw_pr = ymbpw_munics.reset_index()
    ymbpw_pr = ymbpw_pr[ymbpw_pr["bra_id"].map(lambda x: x[:2] == "mg")]
    ymbpw_pr["bra_id"] = ymbpw_pr["bra_id"].astype(str).replace(get_lookup("pr"))
    ymbpw_pr = ymbpw_pr.groupby(["year", "bra_id", "hs", "wld"]).sum()
    
    ymbpw = pd.concat([ymbpw_states, ymbpw_munics, ymbpw_meso, ymbpw_pr])
    
    ymbpw_hs2 = ymbpw.reset_index()
    ymbpw_hs2["hs"] = ymbpw_hs2["hs"].apply(lambda x: x[:2])
    ymbpw_hs2 = ymbpw_hs2.groupby(["year", "bra_id", "hs", "wld"]).sum()
    
    ymbpw_hs4 = ymbpw.reset_index()
    ymbpw_hs4["hs"] = ymbpw_hs4["hs"].apply(lambda x: x[:4])
    ymbpw_hs4 = ymbpw_hs4.groupby(["year", "bra_id", "hs", "wld"]).sum()
    
    ymbpw = pd.concat([ymbpw, ymbpw_hs2, ymbpw_hs4])

    ymbpw_continents = ymbpw.reset_index()
    ymbpw_continents["wld"] = ymbpw_continents["wld"].apply(lambda x: x[:2])
    ymbpw_continents = ymbpw_continents.groupby(["year", "bra_id", "hs", "wld"]).sum()
    
    ymbpw = pd.concat([ymbpw, ymbpw_continents])
    
    ymbpw.index.names = ["year", "bra_id", "hs_id", "wld_id"]
    ymbpw = ymbpw.sortlevel()
    
    # yp = ymbpw.groupby(level=["year", "hs_id"]).sum()
    # print yp.xs([2013, "063808"])
    # sys.exit()
    
    # print ymbpw.head()
    # print ymbpw.xs([2000, "mgplr03"])
    # print ymbpw.shape
    ymbpw.to_csv(bz2.BZ2File(output_file, 'wb'), sep="\t", index=True)

if __name__ == "__main__":
    start = time.time()
    
    aggregate()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;