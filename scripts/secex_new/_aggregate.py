import sys
import pandas as pd

def aggregate(secex_df):
    ymbpw = secex_df.groupby(["year", "month", "state", "munic", "hs", "wld"]).sum()
    
    ymbpw_years = ymbpw.groupby(level=["year", "state", "munic", "hs", "wld"]).sum()
    ymbpw_years["month"] = "00"
    ymbpw_years = ymbpw_years.set_index("month", append=True)
    ymbpw_years = ymbpw_years.reorder_levels(["year", "month", "state", "munic", "hs", "wld"])
    
    ymbpw = pd.concat([ymbpw, ymbpw_years])
    
    ymbpw_states = ymbpw.groupby(level=["year", "month", "state", "hs", "wld"]).sum()
    ymbpw_states.index.names = ["year", "month", "bra_id", "hs", "wld"]
    
    ymbpw_munics = ymbpw.reset_index()
    ymbpw_munics = ymbpw_munics.drop("state", axis=1)
    ymbpw_munics = ymbpw_munics.groupby(["year", "month", "munic", "hs", "wld"]).sum()
    ymbpw_munics.index.names = ["year", "month", "bra_id", "hs", "wld"]
    
    ymbpw_meso = ymbpw_munics.reset_index()
    ymbpw_meso["bra_id"] = ymbpw_meso["bra_id"].apply(lambda x: x[:4])
    ymbpw_meso = ymbpw_meso.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    

    ymbpw = pd.concat([ymbpw_states, ymbpw_munics, ymbpw_meso])
    
    ymbpw_hs2 = ymbpw.reset_index()
    ymbpw_hs2["hs"] = ymbpw_hs2["hs"].apply(lambda x: x[:2])
    ymbpw_hs2 = ymbpw_hs2.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    
    ymbpw_hs4 = ymbpw.reset_index()
    ymbpw_hs4["hs"] = ymbpw_hs4["hs"].apply(lambda x: x[:4])
    ymbpw_hs4 = ymbpw_hs4.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    
    ymbpw = pd.concat([ymbpw, ymbpw_hs2, ymbpw_hs4])

    ymbpw_continents = ymbpw.reset_index()
    ymbpw_continents["wld"] = ymbpw_continents["wld"].apply(lambda x: x[:2])
    ymbpw_continents = ymbpw_continents.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    
    ymbpw = pd.concat([ymbpw, ymbpw_continents])
    
    ymbpw.index.names = ["year", "month", "bra_id", "hs_id", "wld_id"]
    
    ymbpw = ymbpw.sortlevel()
    
    return ymbpw
    