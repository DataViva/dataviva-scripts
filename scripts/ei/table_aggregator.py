# _aggregate.py
import numpy as np
import pandas as pd
import os

def agg_depth(table, table_name, column_dict, aggtable, pk_cols, depth):
    for search_letter, colname in column_dict.items():
        if search_letter in table_name:
            if aggtable.empty:
                aggtable = table.reset_index()
            # print search_letter, table_name
            # print aggtable.head()
            aggtable[colname] = aggtable[colname].apply(lambda x: x[:depth])

    if not aggtable.empty:
        aggtable = aggtable.groupby(pk_cols).aggregate(np.sum)
        return aggtable

    return pd.DataFrame()

def bra_aggregations(table, table_name, pk_cols):
    bra_dict = {'s' : 'Municipality_ID_Sender', 'r': 'Municipality_ID_Receiver' }
    table_meso = agg_depth(table, table_name, bra_dict, pd.DataFrame(), pk_cols, 4)
    table_state = agg_depth(table, table_name, bra_dict, pd.DataFrame(), pk_cols, 2)
    return table_meso, table_state

def cnae_aggregations(table, table_name, pk_cols):
    cnae_dict = {'s' : 'EconomicAtivity_ID_CNAE_Sender', 'r': 'EconomicAtivity_ID_CNAE_Receiver' }
    table_l3= agg_depth(table, table_name, cnae_dict, pd.DataFrame(), pk_cols, 3)
    table_l1 = agg_depth(table, table_name, cnae_dict, pd.DataFrame(), pk_cols, 1)
    return table_l1, table_l3

def hs_aggregations(table, table_name, pk_cols):
    hs_dict = {'p' : 'TransactedProduct_ID_HS' }
    table_l4= agg_depth(table, table_name, hs_dict, pd.DataFrame(), pk_cols, 4)
    table_l2 = agg_depth(table, table_name, hs_dict, pd.DataFrame(), pk_cols, 2)
    return table_l2, table_l4

def year_aggregation(table_data, table_name, pk_cols):
    year_cols = [col for col in pk_cols if col != 'Monthly']
    yearly = table_data.groupby(year_cols).sum()
    yearly["Monthly"] = "00"
    yearly = yearly.set_index("Monthly", append=True)
    yearly = yearly.reorder_levels(pk_cols)
    
    return yearly

def add_column_length(table_data, table_name):
    col_lookup = [ ('s', ['Municipality_ID_Sender', 'EconomicAtivity_ID_CNAE_Sender'] ), 
                  ('r', ['Municipality_ID_Receiver', 'EconomicAtivity_ID_CNAE_Receiver' ]),
                  ('p', ['TransactedProduct_ID_HS'] ) ]
    output_lengths = []
    for index, columns in col_lookup:
        if index in table_name:
            for column in columns:
                cname = column + "_len"
                table_data[cname] = pd.Series( map(lambda x: len(str(x)), table_data.index.get_level_values(column)), index = table_data.index)
                output_lengths.append(cname)

    return table_data, output_lengths

def make_table(ymbibip, table_name, output_values, odir, output_name):
    lookup = {
        "y" : ["Year"],
        "m" : ["Monthly"],
        "s" : ['Municipality_ID_Sender', 'EconomicAtivity_ID_CNAE_Sender'],
        "r" : ['Municipality_ID_Receiver', 'EconomicAtivity_ID_CNAE_Receiver'],
        "p" : ["TransactedProduct_ID_HS"]

    }
    pk_cols = []
    for letter in table_name:
        pk_cols += lookup[letter]
    print "PK_cols" , pk_cols
    table = ymbibip.groupby(pk_cols).aggregate(np.sum)
    big_table = table

    print "Adding column lengths..."
    big_table, len_cols = add_column_length(big_table, table_name)


    output_path = os.path.join(odir, "output_%s_%s.csv" % (table_name, output_name))
    big_table.to_csv(output_path, ";", columns = output_values + len_cols)
    return big_table
