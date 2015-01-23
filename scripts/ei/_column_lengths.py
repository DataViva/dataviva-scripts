# helper_columns.py
import pandas as pd

def add_column_length(table_name, table_data):
    columns = {'s': 'bra_id_s', 'r': 'bra_id_r'}
    cols_added = []
    for index, column in columns.items():
        if index in table_name:
            table_data[column + "_len"] = table_data[column].str.len()
            cols_added.append(column + "_len")
    return table_data, cols_added