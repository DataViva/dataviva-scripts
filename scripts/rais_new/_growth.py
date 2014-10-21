import sys
def calc_growth(tbl, tbl_prev, years_ago=1):
        
    tbl = tbl.sortlevel()
    tbl_prev = tbl_prev.sortlevel()

    '''Growth rate'''
    col_name = "wage_growth"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = (tbl["wage"] / tbl_prev["wage"]) ** (1.0/years_ago) - 1
    
    col_name = "num_emp_growth"
    if years_ago > 1:
        col_name = "{0}_{1}".format(col_name, years_ago)
    tbl[col_name] = (tbl["num_emp"] / tbl_prev["num_emp"]) ** (1.0/years_ago) - 1
    
    return tbl