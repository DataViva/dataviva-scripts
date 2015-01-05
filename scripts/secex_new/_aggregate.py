import sys, os
import pandas as pd
import MySQLdb

def get_lookup(type="bra"):
    ''' Connect to DB '''
    db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    if type == "pr":
        cursor.execute("select bra_id, pr_id from attrs_bra_pr")
        lookup = {r[0]:r[1] for r in cursor.fetchall()}
    return lookup

def aggregate(secex_df):
    ybpw = secex_df.groupby(["year", "state_id", "bra_id", "hs_id", "wld_id"]).sum()
    
    ''' GEOGRAPHY '''
    ybpw_states = ybpw.groupby(level=["year", "state_id", "hs_id", "wld_id"]).sum()
    ybpw_states.index.names = ["year", "bra_id", "hs_id", "wld_id"]
    
    ybpw_munics = ybpw.reset_index()
    ybpw_munics = ybpw_munics.drop("state_id", axis=1)
    ybpw_munics = ybpw_munics.groupby(["year", "bra_id", "hs_id", "wld_id"]).sum()
    
    ybpw_meso = ybpw_munics.reset_index()
    ybpw_meso["bra_id"] = ybpw_meso["bra_id"].apply(lambda x: x[:5])
    ybpw_meso = ybpw_meso.groupby(["year", "bra_id", "hs_id", "wld_id"]).sum()
    
    ybpw_regions = ybpw_munics.reset_index()
    ybpw_regions["bra_id"] = ybpw_regions["bra_id"].apply(lambda x: x[:1])
    ybpw_regions = ybpw_regions.groupby(["year", "bra_id", "hs_id", "wld_id"]).sum()

    ybpw_micro = ybpw_munics.reset_index()
    ybpw_micro["bra_id"] = ybpw_micro["bra_id"].apply(lambda x: x[:7])
    ybpw_micro = ybpw_micro.groupby(["year", "bra_id", "hs_id", "wld_id"]).sum()
    
    ybpw_pr = ybpw_munics.reset_index()
    ybpw_pr = ybpw_pr[ybpw_pr["bra_id"].map(lambda x: x[:3] == "4mg")]
    ybpw_pr["bra_id"] = ybpw_pr["bra_id"].astype(str).replace(get_lookup("pr"))
    ybpw_pr = ybpw_pr.groupby(["year", "bra_id", "hs_id", "wld_id"]).sum()
    
    ybpw = pd.concat([ybpw_regions, ybpw_states, ybpw_munics, ybpw_meso, ybpw_micro, ybpw_pr])
    
    ''' PRODUCTS '''
    ybpw_hs2 = ybpw.reset_index()
    ybpw_hs2["hs_id"] = ybpw_hs2["hs_id"].apply(lambda x: x[:2])
    ybpw_hs2 = ybpw_hs2.groupby(["year", "bra_id", "hs_id", "wld_id"]).sum()
    
    ybpw = pd.concat([ybpw, ybpw_hs2])
    
    ''' COUNTRIES '''
    ybpw_continents = ybpw.reset_index()
    ybpw_continents["wld_id"] = ybpw_continents["wld_id"].apply(lambda x: x[:2])
    ybpw_continents = ybpw_continents.groupby(["year", "bra_id", "hs_id", "wld_id"]).sum()
    
    ybpw = pd.concat([ybpw, ybpw_continents])
    
    ybpw = ybpw.sortlevel()
    return ybpw
    