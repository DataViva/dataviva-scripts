import sys
def calc_growth(tbl, tbl_prev, years_ago=1):
    
    '''Growth value'''
    col_name = "export_val_growth_val"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = tbl["export_val"] - tbl_prev["export_val"]
    
    # print tbl_prev.index.is_unique
    # print tbl.index.is_unique

    '''Growth rate'''
    col_name = "export_val_growth_pct"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = (tbl["export_val"] / tbl_prev["export_val"]) ** (1.0/years_ago) - 1
    
    return tbl