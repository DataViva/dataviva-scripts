import sys, os
import pandas as pd
import MySQLdb

def get_lookup(type="bra"):
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    if type == "pr":
        cursor.execute("select bra_id, pr_id from attrs_bra_pr")
        lookup = {r[0]:r[1] for r in cursor.fetchall()}
    return lookup

def aggregate(secex_df):
    ymbpw = secex_df.groupby(["year", "month", "state", "munic", "hs", "wld"]).sum()
    
    ''' TIME '''
    ymbpw_years = ymbpw.groupby(level=["year", "state", "munic", "hs", "wld"]).sum()
    ymbpw_years["month"] = "00"
    ymbpw_years = ymbpw_years.set_index("month", append=True)
    ymbpw_years = ymbpw_years.reorder_levels(["year", "month", "state", "munic", "hs", "wld"])
    
    ymbpw = pd.concat([ymbpw, ymbpw_years])
    
    ''' GEOGRAPHY '''
    ymbpw_states = ymbpw.groupby(level=["year", "month", "state", "hs", "wld"]).sum()
    ymbpw_states.index.names = ["year", "month", "bra_id", "hs", "wld"]
    
    ymbpw_munics = ymbpw.reset_index()
    ymbpw_munics = ymbpw_munics.drop("state", axis=1)
    ymbpw_munics = ymbpw_munics.groupby(["year", "month", "munic", "hs", "wld"]).sum()
    ymbpw_munics.index.names = ["year", "month", "bra_id", "hs", "wld"]
    
    ymbpw_meso = ymbpw_munics.reset_index()
    ymbpw_meso["bra_id"] = ymbpw_meso["bra_id"].apply(lambda x: x[:4])
    ymbpw_meso = ymbpw_meso.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    
    ymbpw_pr = ymbpw_munics.reset_index()
    ymbpw_pr = ymbpw_pr[ymbpw_pr["bra_id"].map(lambda x: x[:2] == "mg")]
    ymbpw_pr["bra_id"] = ymbpw_pr["bra_id"].astype(str).replace(get_lookup("pr"))
    ymbpw_pr = ymbpw_pr.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    
    ymbpw = pd.concat([ymbpw_states, ymbpw_munics, ymbpw_meso, ymbpw_pr])
    
    ''' PRODUCTS '''
    ymbpw_hs2 = ymbpw.reset_index()
    ymbpw_hs2["hs"] = ymbpw_hs2["hs"].apply(lambda x: x[:2])
    ymbpw_hs2 = ymbpw_hs2.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    
    ymbpw_hs4 = ymbpw.reset_index()
    ymbpw_hs4["hs"] = ymbpw_hs4["hs"].apply(lambda x: x[:4])
    ymbpw_hs4 = ymbpw_hs4.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    
    ymbpw = pd.concat([ymbpw, ymbpw_hs2, ymbpw_hs4])
    
    ''' COUNTRIES '''
    ymbpw_continents = ymbpw.reset_index()
    ymbpw_continents["wld"] = ymbpw_continents["wld"].apply(lambda x: x[:2])
    ymbpw_continents = ymbpw_continents.groupby(["year", "month", "bra_id", "hs", "wld"]).sum()
    
    ymbpw = pd.concat([ymbpw, ymbpw_continents])
    
    ymbpw.index.names = ["year", "month", "bra_id", "hs_id", "wld_id"]
    ymbpw = ymbpw.sortlevel()
    return ymbpw
    