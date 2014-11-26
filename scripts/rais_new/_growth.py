import sys, os
from _to_df import to_df

def extract_tbl_name(path):
    file_name = os.path.basename(path)
    return file_name.replace(".tsv.bz2", "")

def calc_growth(year, current_year_file_path, prev_year_file_path, prev5_year_file_path):
    tbl_name = extract_tbl_name(current_year_file_path)
    current_year_tbl = to_df(current_year_file_path, tbl_name)
    
    for prev_path, years_ago in [(prev_year_file_path, 1), (prev5_year_file_path, 5)]:
        if not prev_path: continue
        prev_year_tbl = to_df(prev_path, tbl_name)
    
        prev_year_tbl = prev_year_tbl.reset_index(level="year")
        prev_year_tbl["year"] = int(year)
        prev_year_tbl = prev_year_tbl.set_index("year", append=True)
        prev_year_tbl = prev_year_tbl.reorder_levels(["year"] + list(prev_year_tbl.index.names)[:-1])
    
        current_year_tbl = do_growth(current_year_tbl, prev_year_tbl, years_ago)
    
    return (tbl_name, current_year_tbl)

def do_growth(tbl, tbl_prev, years_ago=1):
        
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