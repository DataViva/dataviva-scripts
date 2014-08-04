import os, MySQLdb
import pandas as pd

def replace_vals(secex_df, missing=None, debug=False):
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    def get_lookup(type="bra"):
        if type == "bra":
            cursor.execute("select id_mdic, id from attrs_bra where id_mdic is not null and length(id) = 8;")
            lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
            lookup["4314548"] = "rs030014"
            lookup["9999999"] = "xx000007"
            lookup["9400000"] = "xx000002"
        elif type == "state":
            cursor.execute("select id_mdic, id from attrs_bra where id_mdic is not null and length(id) = 2;")
            lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
            lookup["94"] = "xx"
            lookup["95"] = "xx"
            lookup["96"] = "xx"
            lookup["97"] = "xx"
            lookup["98"] = "xx"
            lookup["99"] = "xx"
        elif type == "hs":
            cursor.execute("select substr(id, 3), id from attrs_hs where substr(id, 3) != '' and length(id) = 6;")
            lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
            lookup["9991"] = "229999"
            lookup["9992"] = "229999"
            lookup["9998"] = "229999"
            lookup["9997"] = "229999"
        elif type == "wld":
            cursor.execute("select id_mdic, id from attrs_wld where id_mdic is not null and length(id) = 5;")
            lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
            lookup["695"] = "nakna"
            lookup["423"] = "asmys"
            lookup["152"] = "euchi"
        return lookup
    
    replacements = [
        {"col":"munic", "lookup":get_lookup("bra")},
        {"col":"state", "lookup":get_lookup("state")},
        {"col":"hs", "lookup":get_lookup("hs")},
        {"col":"wld", "lookup":get_lookup("wld")}
    ]
    
    secex_df = secex_df.reset_index()
    for r in replacements:
        # print "Replacing {0} IDs".format(r["col"])
        num_rows = secex_df.shape[0]
        secex_df[r["col"]] = secex_df[r["col"]].astype(str).replace(r["lookup"])
        not_in_db = set(secex_df[r["col"]].unique()).difference(set(r["lookup"].values()))
        if not_in_db:
            if missing != None:
                if r["col"] in missing:
                    missing[r["col"]] = missing[r["col"]].union(not_in_db)
                else:
                    missing[r["col"]] = not_in_db
            else:
                print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(r["col"]);
                drop_criterion = secex_df[r["col"]].map(lambda x: x not in not_in_db)
                secex_df = secex_df[drop_criterion]
                print not_in_db; print "{0} rows deleted.".format(num_rows - secex_df.shape[0]); print;

    if missing != None:
        return missing
    
    return secex_df