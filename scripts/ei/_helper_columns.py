# helper_columns.py
import pandas as pd

def add_helper_cols(table_name, table_data):
    columns = {'s': ['bra_id_s', 'cnae_id_s'], 'r': ['bra_id_r', 'cnae_id_r'], 'p': ['hs_id'] }

    sizes = {"bra_id_s": [1, 3], "bra_id_r": [1, 3], "hs_id": [2], "cnae_id_s":[1], "cnae_id_r" : [1] }

    for index, column_list in columns.items():
        if index in table_name:
            print "INDEX in table"
            for column in column_list:
                print column, "COL"
                if column in sizes:
                    mysizes = sizes[column]
                    for i in mysizes:
                        table_data[ column + str(i) ] = pd.Series( map(lambda x: str(x)[:i], table_data.index.get_level_values(column)), index = table_data.index)
                    # print table_data.head()
    return table_data