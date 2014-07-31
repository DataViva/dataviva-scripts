import os, sys, MySQLdb
import pandas as pd

def replace_vals(df, missing={}, debug=False):
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA2_DB_USER"], 
                            passwd=os.environ["DATAVIVA2_DB_PW"], 
                            db=os.environ["DATAVIVA2_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    def get_bra_lookup():
        cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 8;")
        return {str(r[0]):r[1] for r in cursor.fetchall()}
    
    def get_course_lookup():
        cursor.execute("select CAST(id AS UNSIGNED) as id_raw, id from attrs_course;")
        courses = {str(r[0]):r[1] for r in cursor.fetchall()}
        courses[''] = "00000"
        return courses
    
    replacements = [
        {"col":"munic", "lookup":get_bra_lookup()},
        {"col":"course_id", "lookup":get_course_lookup()}
    ]
    
    df = df.set_index([r["col"] for r in replacements])
    index_as_tuples = list(df.index)
    
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
    df.index = new_index
    
    df = df.reset_index()
    
    if missing:
        for col, vals in missing.items():
            num_rows = df.shape[0]
            print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(col);
            print vals
            drop_criterion = df[col].map(lambda x: x not in vals)
            df = df[drop_criterion]
            print; print "{0} rows deleted.".format(num_rows - df.shape[0]); print;
    
    return df