import sys, os
import pandas as pd

def aggregate(secex_df):
    ymbpw = secex_df.groupby(level=["year", "month", "state_id", "bra_id", "hs_id", "wld_id"]).sum()
    
    ''' TIME '''
    print "years..."
    ymbpw_years = ymbpw.groupby(level=["year", "state_id", "bra_id", "hs_id", "wld_id"]).sum()
    ymbpw_years["month"] = "00"
    ymbpw_years = ymbpw_years.set_index("month", append=True)
    ymbpw_years = ymbpw_years.reorder_levels(["year", "month", "state_id", "bra_id", "hs_id", "wld_id"])
    
    ymbpw = pd.concat([ymbpw, ymbpw_years])
    
    ''' GEOGRAPHY '''
    print "states..."
    ymbpw_states = ymbpw.groupby(level=["year", "month", "state_id", "hs_id", "wld_id"]).sum()
    ymbpw_states.index.names = ["year", "month", "bra_id", "hs_id", "wld_id"]
    
    print "munics..."
    ymbpw_munics = ymbpw.reset_index()
    ymbpw_munics = ymbpw_munics.drop("state_id", axis=1)
    ymbpw_munics = ymbpw_munics.groupby(["year", "month", "bra_id", "hs_id", "wld_id"]).sum()
    
    print "mesos..."
    ymbpw_meso = ymbpw_munics.reset_index()
    ymbpw_meso["bra_id"] = ymbpw_meso["bra_id"].apply(lambda x: x[:5])
    ymbpw_meso = ymbpw_meso.groupby(["year", "month", "bra_id", "hs_id", "wld_id"]).sum()
    
    print "regions..."
    ymbpw_regions = ymbpw_munics.reset_index()
    ymbpw_regions["bra_id"] = ymbpw_regions["bra_id"].apply(lambda x: x[:1])
    ymbpw_regions = ymbpw_regions.groupby(["year", "month", "bra_id", "hs_id", "wld_id"]).sum()

    print "micro..."
    ymbpw_micro = ymbpw_munics.reset_index()
    ymbpw_micro["bra_id"] = ymbpw_micro["bra_id"].apply(lambda x: x[:7])
    ymbpw_micro = ymbpw_micro.groupby(["year", "month", "bra_id", "hs_id", "wld_id"]).sum()
    
    print "concatenating..."
    ymbpw = pd.concat([ymbpw_regions, ymbpw_states, ymbpw_munics, ymbpw_meso, ymbpw_micro])
    
    ''' PRODUCTS '''
    ymbpw_hs2 = ymbpw.reset_index()
    ymbpw_hs2["hs_id"] = ymbpw_hs2["hs_id"].apply(lambda x: x[:2])
    ymbpw_hs2 = ymbpw_hs2.groupby(["year", "month", "bra_id", "hs_id", "wld_id"]).sum()
    
    # ymbpw_hs4 = ymbpw.reset_index()
    # ymbpw_hs4["hs_id"] = ymbpw_hs4["hs_id"].apply(lambda x: x[:4])
    # ymbpw_hs4 = ymbpw_hs4.groupby(["year", "month", "bra_id", "hs_id", "wld_id"]).sum()
    
    ymbpw = pd.concat([ymbpw, ymbpw_hs2])
    
    ''' COUNTRIES '''
    ymbpw_continents = ymbpw.reset_index()
    ymbpw_continents["wld_id"] = ymbpw_continents["wld_id"].apply(lambda x: x[:2])
    ymbpw_continents = ymbpw_continents.groupby(["year", "month", "bra_id", "hs_id", "wld_id"]).sum()
    
    ymbpw = pd.concat([ymbpw, ymbpw_continents])
    
    ymbpw = ymbpw.sortlevel()
    return ymbpw
    