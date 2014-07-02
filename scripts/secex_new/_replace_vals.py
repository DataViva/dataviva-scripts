import os, MySQLdb
import pandas as pd

def replace_vals(secex_df, debug=False):
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    def get_bra_lookup():
        cursor.execute("select id_mdic, id from attrs_bra where id_mdic is not null and length(id) = 8;")
        return {str(r[0]):r[1] for r in cursor.fetchall()}

    def get_state_lookup():
        cursor.execute("select id_mdic, id from attrs_bra where id_mdic is not null and length(id) = 2;")
        return {str(r[0]):r[1] for r in cursor.fetchall()}

    def get_hs_lookup():
        cursor.execute("select substr(id, 3), id from attrs_hs where substr(id, 3) != '' and length(id) = 6;")
        return {str(r[0]):r[1] for r in cursor.fetchall()}

    def get_wld_lookup():
        cursor.execute("select id_mdic, id from attrs_wld where id_mdic is not null and length(id) = 5;")
        return {str(r[0]):r[1] for r in cursor.fetchall()}
    
    replacements = [
        {"col":"munic", "lookup":get_bra_lookup()},
        {"col":"state", "lookup":get_state_lookup()},
        {"col":"hs", "lookup":get_hs_lookup()},
        {"col":"wld", "lookup":get_wld_lookup()}
    ]
    
    secex_df = secex_df.reset_index()
    for r in replacements:
        # print "Replacing {0} IDs".format(r["col"])
        num_rows = secex_df.shape[0]
        secex_df[r["col"]] = secex_df[r["col"]].astype(str).replace(r["lookup"])
        not_in_db = set(secex_df[r["col"]].unique()).difference(set(r["lookup"].values()))
        if not_in_db:
            print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(r["col"]);
            drop_criterion = secex_df[r["col"]].map(lambda x: x not in not_in_db)
            secex_df = secex_df[drop_criterion]
            print not_in_db; print "{0} rows deleted.".format(num_rows - secex_df.shape[0]); print;

    return secex_df