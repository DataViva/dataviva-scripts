import sys, os
import pandas as pd
import MySQLdb

def val_per_unit(ymp, raw_df, trade_flow):
    
    ''' Connect to DB '''
    db = MySQLdb.connect(user=os.environ["DATAVIVA2_DB_USER"], db=os.environ["DATAVIVA2_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    cursor.execute("select id, unit from attrs_hs where unit is not null;")
    hs_unit = {r[0]:r[1] for r in cursor.fetchall()}
    
    raw_df["allowed"] = raw_df["hs_id"]
    raw_df["allowed"] = raw_df["allowed"].replace(hs_unit).astype(int)

    raw_df['allowed'] = raw_df['allowed'].eq(raw_df['unit'])
    raw_df = raw_df[raw_df['allowed']]
    
    raw_df = raw_df[raw_df['val_unit']>0]
    
    raw_df_year = raw_df.groupby(["year", "hs_id"]).sum()
    raw_df_year["month"] = "00"
    raw_df_year = raw_df_year.reset_index()
    raw_df = pd.concat([raw_df_year, raw_df])
    
    raw_df["unit_price"] = raw_df['val_usd'] / raw_df['val_unit']
    raw_df = raw_df[["year", "month", "hs_id", "unit_price"]]
    
    raw_df = raw_df.groupby(["year", "month", "hs_id"]).median()
    
    ymp[trade_flow+"_val_unit"] = raw_df["unit_price"]
    
    # print ymp.head()
    # ymp.to_csv("unit_price.csv")
    # sys.exit()
    return ymp