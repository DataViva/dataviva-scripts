import os, sys, MySQLdb
import pandas as pd

def replace_vals(rais_df, missing={}, debug=False):
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    def get_bra_lookup():
        cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 8;")
        return {str(r[0])[:-1]:r[1] for r in cursor.fetchall()}
    
    def get_cbo_lookup():
        cursor.execute("select id, id from attrs_cbo where length(id) = 6;")
        return {r[0]:r[1] for r in cursor.fetchall()}
    
    def get_cnae_lookup():
        cursor.execute("select substr(id, 2), id from attrs_cnae where length(id) = 6;")
        return {r[0]:r[1] for r in cursor.fetchall()}
    
    replacements = [
        {"col":"munic", "lookup":get_bra_lookup()},
        {"col":"cnae", "lookup":get_cnae_lookup()},
        {"col":"cbo", "lookup":get_cbo_lookup()}
    ]
    
    rais_df = rais_df.set_index([r["col"] for r in replacements])
    index_as_tuples = list(rais_df.index)
    
    for i, raw_tuple in enumerate(index_as_tuples):
        new_tuple = list(raw_tuple)
        
        for ri, r in enumerate(replacements):
            try:
                replacement = r["lookup"][str(raw_tuple[ri])]
            except KeyError:
                replacement = str(raw_tuple[ri])
                if missing and r["col"] in missing:
                    missing[r["col"]].add(str(raw_tuple[ri]))
                else:
                    missing[r["col"]] = set([str(raw_tuple[ri])])
            new_tuple[ri] = replacement
        
        index_as_tuples[i] = tuple(new_tuple)
    
    
    new_index = pd.MultiIndex.from_tuples(index_as_tuples, names=[r["col"] for r in replacements])
    rais_df.index = new_index
    
    # for r in replacements:
    #     # print "Replacing {0} IDs".format(r["col"])
    #     num_rows = rais_df.shape[0]
    #     rais_df[r["col"]] = rais_df[r["col"]].astype(str).replace(r["lookup"])
    #     not_in_db = set(rais_df[r["col"]].unique()).difference(set(r["lookup"].values()))
    #     if not_in_db:
    #         if missing != None:
    #             if r["col"] in missing:
    #                 missing[r["col"]] = missing[r["col"]].union(not_in_db)
    #             else:
    #                 missing[r["col"]] = not_in_db
    #         else:
    #             print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(r["col"]);
    #             drop_criterion = rais_df[r["col"]].map(lambda x: x not in not_in_db)
    #             rais_df = rais_df[drop_criterion]
    #             print not_in_db; print "{0} rows deleted.".format(num_rows - rais_df.shape[0]); print;
    
    rais_df = rais_df.reset_index()
    
    if missing:
        for col, vals in missing.items():
            num_rows = rais_df.shape[0]
            print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(col);
            print vals
            drop_criterion = rais_df[col].map(lambda x: x not in vals)
            rais_df = rais_df[drop_criterion]
            print; print "{0} rows deleted.".format(num_rows - rais_df.shape[0]); print;
    
    return rais_df