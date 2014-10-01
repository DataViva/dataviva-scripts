import pandas as pd

def add_column_length(table_name, table_data):
    indicies = [('w', 'wld_id'), ('p', 'hs_id'), ('b', 'bra_id')]
    for index, column in indicies:
        if index in table_name:
            table_data[column + "_len"] = pd.Series( map(lambda x: len(str(x)), table_data.index.get_level_values(column)), index = table_data.index)
            cols = table_data.columns.tolist()
            cols = [column + "_len"] + cols[:-1]
            table_data = table_data[cols]
    return table_data