import sys
def calc_growth(tbl, tbl_prev, years_ago=1):
        
    tbl = tbl.sortlevel()
    tbl_prev = tbl_prev.sortlevel()
    
    '''Growth value'''
    col_name = "wage_growth_val"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = tbl["wage"] - tbl_prev["wage"]
    
    col_name = "num_emp_growth_val"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = tbl["num_emp"] - tbl_prev["num_emp"]
    
    # print tbl_prev.index.is_unique
    # print tbl.index.is_unique

    '''Growth rate'''
    col_name = "wage_growth_pct"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = (tbl["wage"] / tbl_prev["wage"]) ** (1.0/years_ago) - 1
    
    col_name = "num_emp_growth_pct"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = (tbl["num_emp"] / tbl_prev["num_emp"]) ** (1.0/years_ago) - 1
    
    return tbl