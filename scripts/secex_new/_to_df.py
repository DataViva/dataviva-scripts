import pandas as pd
from ..helpers import get_file

def to_df(input_file_path, index=False, debug=False):
    input_file = get_file(input_file_path)
    
    if index:
        index_lookup = {"y":"year", "m":"month", "b":"bra_id", "p":"hs_id", "w":"wld_id"}
        index_cols = [index_lookup[i] for i in index]
        secex_df = pd.read_csv(input_file, sep="\t", converters={"month":str, "hs_id":str})
        secex_df = secex_df.set_index(index_cols)
    else:
        cols = ["year", "month", "wld", "state", "customs", "munic", "unit", \
                "quantity", "val_kg", "val_usd", "hs"]
        delim = "|"
        secex_df = pd.read_csv(input_file, header=0, sep=delim, converters={"month":str, "hs":str}, names=cols)    
        secex_df = secex_df[["year", "month", "state", "munic", "hs", "wld", "val_usd"]]

    
    return secex_df