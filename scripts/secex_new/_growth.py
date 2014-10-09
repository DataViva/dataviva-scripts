import sys
def calc_growth(tbl, tbl_prev, years_ago=1):

    '''Export Growth rate'''
    col_name = "export_val_growth"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = (tbl["export_val"] / tbl_prev["export_val"]) ** (1.0/years_ago) - 1
    
    '''Import Growth rate'''
    col_name = "import_val_growth"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = (tbl["import_val"] / tbl_prev["import_val"]) ** (1.0/years_ago) - 1
    
    return tbl