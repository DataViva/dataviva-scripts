# _aggregate.py
import numpy as np
import pandas as pd
import os
from pandas.tools.pivot import pivot_table
import itertools
from _helper_columns import add_helper_cols


BIGGEST_TABLE = 'ymsrp'

def pk(table_name):
    ''' Determine which columns are part of the primary key, based on table name'''
    lookup = {
        "s" : ['bra_id_s', 'cnae_id_s'],
        "r" : ['bra_id_r', 'cnae_id_r'],
        "p" : ["hs_id"],
        "c" : ["cfop_class"]
    }
    pk_cols = []
    for letter in table_name:
        pk_cols += lookup[letter]
    print "PK_cols" , pk_cols
    return pk_cols


def agg_depths(first, t_name):
    table= first.reset_index()
    geo_depths = [1, 3, 9]
    cnae_depths = [3]
    product_depths = [2, 6]
    
    
    # nestings = {"s": geo_depths, "r": geo_depths, "p": product_depths}
    # lookup = {"s": "bra_id_s", "r": "bra_id_r", "p":"hs_id"}
    my_nesting = [] # [nestings[i] for i in t_name if i not in ["y", "m"] ]
    my_nesting_cols = []

    for letter in t_name:
        if letter in ["s", "r"]:
            my_nesting.append(geo_depths) 
            my_nesting.append(cnae_depths)
            my_nesting_cols.append("bra_id_" + letter)
            my_nesting_cols.append("cnae_id_" + letter)
        elif letter == "p":
            my_nesting.append(product_depths)
            my_nesting_cols.append("hs_id")

    # my_nesting_cols = [lookup[i] for i in t_name if i not in ["y", "m"]]
    print my_nesting, my_nesting_cols
    print
    mynewtable = pd.DataFrame()     

    for depths in itertools.product(*my_nesting):
        my_pk = []

        for col_name, d in zip(my_nesting_cols, depths):
            my_pk.append( table[col_name].str.slice(0, d) )
            

        moi = table.groupby(my_pk, sort=False).agg( np.sum )
        mynewtable = pd.concat([mynewtable, moi])
        print "done ", depths , " table"

    return mynewtable

def make_table(ymbibip, table_name, output_values, odir, output_name, ignore_list=[], year=2013, month=-1):
    print table_name, "table in progress..."

    pk_cols = pk(table_name)
    print "table name", table_name, "pks=",pk_cols
    # if table_name == BIGGEST_TABLE:
    ymbibip = ymbibip.reset_index()

    big_table = ymbibip.groupby(pk_cols).sum()

    print "ADDING HELPERS!"
    big_table = add_helper_cols(table_name, big_table)
    
    tmp = output_values
    # big_table = big_table.reset_index()
    if "r" in table_name:
        tmp = tmp + ["bra_id_r1", "bra_id_r3", "cnae_id_r1"]
    if "s" in table_name:
        tmp = tmp + ["bra_id_s1", "bra_id_s3", "cnae_id_s1"] 
    if "p" in table_name:
        tmp= tmp + ["hs_id2"]
    
    big_table["year"] = year
    big_table["month"] = month

    print "Writing csv to disk..."

    output_path = os.path.join(odir, "output_%s_%s.csv" % (table_name, output_name))
    big_table.to_csv(output_path, ";", columns = tmp)
    return big_table
