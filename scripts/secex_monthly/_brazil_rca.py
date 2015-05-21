import pandas as pd

def brazil_rca(ymp, ypw_file_path, year):
    
    bra_rcas = pd.read_csv(ypw_file_path, compression="bz2", sep="\t", converters={"hs_id":str})
    bra_rcas = bra_rcas[bra_rcas['wld_id'] == "sabra"]
    
    bra_rcas["month"] = "00"
    bra_rcas = bra_rcas.set_index(["year", "month", "hs_id"])
    
    ymp["rca_wld"] = bra_rcas["rca"]
    
    return ymp