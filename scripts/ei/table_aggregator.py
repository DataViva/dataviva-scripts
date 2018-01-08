# _aggregate.py
import numpy as np
import pandas as pd
import os
from pandas.tools.pivot import pivot_table
import itertools
from database import DB
from _column_lengths import add_column_length


def setup_transfomers():
    db = DB()
    transformers = {
        "planning_region": {
            "table": "attrs_bra_pr",
            "key": "bra_id",
            "value": "pr_id"
        }
    }
    db_converters = {colname: db.make_dict(**settings)
                    for colname, settings in transformers.items()}
    return db_converters

transformed_depths = setup_transfomers()

def pk(table_name):
    ''' Determine which columns are part of the primary key, based on table name'''
    lookup = {
        "y" : ["year"],
        "m" : ["month"],
        "s" : ['bra_id_s'],
        "r" : ['bra_id_r'],
        "p" : ["hs_id"],
        "c" : ["cfop_class"]
    }
    pk_cols = []
    for letter in table_name:
        pk_cols += lookup[letter]
    print "PK_cols" , pk_cols
    return pk_cols

def year_aggregation(table_data, table_name, pk_cols):
    year_cols = [col for col in pk_cols if col != 'month']
    yearly = table_data.groupby(level=year_cols).sum()
    yearly["month"] = "00"
    yearly = yearly.set_index("month", append=True)
    yearly = yearly.reorder_levels(pk_cols)
    
    return yearly

def agg_depths(first, t_name):

    table= first.reset_index()
    geo_depths = [1, 3, 5, 7, "planning_region", 9]
    
    my_nesting = []
    my_nesting_cols = []

    for letter in t_name:
        if letter in ["s", "r"]:
            my_nesting.append(geo_depths) 
            my_nesting_cols.append("bra_id_" + letter)

    print my_nesting, my_nesting_cols
    print
    mynewtable = pd.DataFrame()     

    for depths in itertools.product(*my_nesting):
        my_pk = [table["year"], table["month"]]

        for col_name, d in zip(my_nesting_cols, depths):
            if type(d) == str:
                transformation = transformed_depths[d]
                my_pk.append( table[col_name].map(transformation) )
            else:
                my_pk.append(table[col_name].str.slice(0, d))

        moi = table.groupby(my_pk, sort=False).agg( np.sum )
        mynewtable = pd.concat([mynewtable, moi])
        print "Done: ", depths , " table"

    return mynewtable

def make_table(ymbibip, table_name, output_values, odir, output_name, ignore_list=[]):
    print table_name, "table in progress..."
    pk_cols = pk(table_name)

    print "table name", table_name, "pks=",pk_cols
    ymbibip = ymbibip.reset_index()

    big_table = ymbibip.groupby(pk_cols).sum()
    
    # big_table = big_table.reset_index()

    big_table = agg_depths(ymbibip, table_name)

    print "Writing csv to disk..."
    big_table, added_cols = add_column_length(table_name, big_table)
    tmp = output_values + added_cols
    
    output_path = os.path.join(odir, "output_%s_%s.csv" % (table_name, output_name))
    big_table.to_csv(output_path, ";", columns = tmp)
    return big_table
