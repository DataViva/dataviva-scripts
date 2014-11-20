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
        cursor.execute("select id, id from attrs_course_hedu;")
        courses = {str(r[0]):r[1] for r in cursor.fetchall()}
        return courses
    
    replacements = [
        {"col":"munic", "lookup":get_bra_lookup()},
        {"col":"course_hedu_id", "lookup":get_course_lookup()}
    ]
    
    df = df.set_index([r["col"] for r in replacements])
    indexes = list(df.index)

    for i, index in enumerate(indexes):
        if type(index) == tuple: index = list(index)
        else: index = [index]
        for ri, r in enumerate(replacements):
            try:
                replacement = r["lookup"][str(index[ri])]
            except KeyError:
                replacement = str(index[ri])
                if missing and r["col"] in missing:
                    missing[r["col"]].add(str(index[ri]))
                else:
                    missing[r["col"]] = set([str(index[ri])])
            index[ri] = replacement

        if len(replacements) > 1: indexes[i] = tuple(index)
        else: indexes[i] = index[0]

    if len(replacements) > 1:
        indexes = pd.MultiIndex.from_tuples(indexes, names=[r["col"] for r in replacements])
        df.index = indexes
    else:
        df.index = indexes
        df.index.name = r["col"]

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