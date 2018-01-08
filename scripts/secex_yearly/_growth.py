import sys
def calc_growth(tbl, tbl_prev, years_ago=1):

    '''Growth rate'''
    col_name = "val_usd_growth"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = (tbl["val_usd"] / tbl_prev["val_usd"]) ** (1.0/years_ago) - 1
    
    return tbl