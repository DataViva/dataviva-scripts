# helper_columns.py
import pandas as pd

def add_column_length(table_name, table_data):
    print "TABLENAME=", table_name
    columns = {'s': 'bra_id_s', 'r': 'bra_id_r'}
    cols_added = []
    for index, column in columns.items():
        if index in table_name:
            table_data[column + "_len"] = pd.Series( map(lambda x: len(str(x)), table_data.index.get_level_values(column)), index = table_data.index)
            cols_added.append(column + "_len")
    return table_data, cols_added
