import os, sys, MySQLdb
import pandas as pd

def replace_vals(df, missing={}, debug=False):
    ''' Connect to DB '''
    db = MySQLdb.connect(host=os.environ.get("DATAVIVA2_DB_HOST", "localhost"), 
                         user=os.environ["DATAVIVA2_DB_USER"], 
                         passwd=os.environ["DATAVIVA2_DB_PW"], 
                         db=os.environ["DATAVIVA2_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    def get_bra_lookup():
        cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 8;")
        return {str(r[0]):r[1] for r in cursor.fetchall()}

    # def get_distances():
    #     print "Getting distances..."
    #     cursor.execute("select bra_id_origin, bra_id_dest, distance from attrs_bb;")
    #     data = {}
    #     for r in cursor.fetchall():
    #         src = str(r[0])
    #         target = str(r[1])
    #         dist = float(r[2])
    #         if not src in data:
    #             data[src] = {}
    #         data[src][target] = dist
    #     print "Finished distances"
    #     return data


    b_lookup = get_bra_lookup()
    replacements = [
        {"col":"munic", "lookup": b_lookup},
        # {"col":"munic_lives", "lookup": b_lookup},
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
    


    # bra_distances = get_distances()

    # def compute_distance(row):
    #     src = row['munic']
    #     target = row['munic_lives']
    #     if not src in bra_distances or not target in bra_distances[src]:
    #         return -1 # bad data
    #     return bra_distances[src][target]
    
    # rais_df['distance'] = rais_df.apply(compute_distance)


    return df