# _aggregate.py
import numpy as np
import pandas as pd
import os
from pandas.tools.pivot import pivot_table

BIGGEST_TABLE = 'ymsrp'

def pk(table_name):
    ''' Determine which columns are part of the primary key, based on table name'''
    lookup = {
        "y" : ["year"],
        "m" : ["month"],
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

def year_aggregation(table_data, table_name, pk_cols):
    year_cols = [col for col in pk_cols if col != 'month']
    yearly = table_data.groupby(level=year_cols).sum()
    yearly["month"] = "00"
    yearly = yearly.set_index("month", append=True)
    yearly = yearly.reorder_levels(pk_cols)
    
    return yearly

def make_table(ymbibip, table_name, output_values, odir, output_name, ignore_list=[]):
    pk_cols = pk(table_name)
    
    if table_name == BIGGEST_TABLE:
        table = ymbibip.groupby(pk_cols).aggregate(np.sum)
    else:
        table = ymbibip.groupby(level=pk_cols).aggregate(np.sum)

    table["net_value"] = table["purchase_value"] + table["service_value"] + table["fixed_asset_value"] + table["other_value"] - table["devolution_value"]
    output_values.append("net_value")

    yearly = year_aggregation(table, table_name, pk_cols)

    big_table = pd.concat([table, yearly])
    print "Writing csv to disk..."

    output_path = os.path.join(odir, "output_%s_%s.csv" % (table_name, output_name))
    big_table.to_csv(output_path, ";", columns = output_values)
    return big_table
