import sys, bottleneck, MySQLdb, os
import pandas as pd
import numpy as np
import re

agg_rules = {
    "age" : np.mean,
    "enroll_id": pd.Series.count,
    "class_id": pd.Series.nunique,
}

commute_rules = {
    "commute_distance" : np.mean
}

# pk = ["year", "bra_id", "school_id", "course_id", "d_id"]

db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
distances = {} 

def get_planning_regions():
    ''' Connect to DB '''
    cursor = db.cursor()
    cursor.execute("select bra_id, pr_id from attrs_bra_pr")
    return {r[0]:r[1] for r in cursor.fetchall()}

planning_regions = get_planning_regions()

# def query_distance(src, target, distances):
#     if src == target:
#         return 0

#     if not src or not target:
#         return None

#     src = src[1:]
#     target = target[1:]
#     if src in distances and target in distances[src]:
#         return distances[src][target]
#     elif target in distances and src in distances[target]:
#         return distances[target][src]
#     cursor = db.cursor()
#     q="SELECT distance FROM attrs_bb WHERE bra_id_origin = '%s' AND bra_id_dest ='%s' " % (src,target)
#     cursor.execute(q)
#     res = cursor.fetchall()
#     if res and res[0]:
#         val = res[0][0]
#         if not src in distances: distances[src] = {}
#         distances[src][target] = val
#         return val
   
#     print "MISSING", src, target
#     # return None
#     raise Exception("problem calculating distance `%s`,`%s`." % (src, target) )

# def compute_distances(tbl):
#     tbl["commute_distance"] = tbl.apply(lambda x: query_distance(x.bra_id_lives, x.bra_id, distances), axis=1)    
#     return tbl

def aggregate(this_pk, tbl, dem, cid_len=None):

    if dem:
        tbl["d_id"] = tbl[dem]
        # this_pk = pk
    # else:
        # this_pk = pk[:-1]
    
    # print "getting distances..."

    tbl = tbl.drop(['gender', 'color', 'loc', 'school_type'], axis=1)
    
    if cid_len:
        tbl['course_id'] = tbl["course_id"].str.slice(0, cid_len)

    # tbl = compute_distances(tbl)
    print "Step A."
    tbl_all = tbl.groupby(this_pk).agg(agg_rules)
    # print tbl_all[tbl_all.commute_distance > 0].head()

    print "Step B."
    tbl_region = tbl.reset_index()
    tbl_region["bra_id"] = tbl_region["bra_id"].str.slice(0, 1)
    tbl_region = tbl_region.groupby(this_pk).agg(agg_rules)
    
    print "Step C."
    tbl_state = tbl.reset_index()
    tbl_state["bra_id"] = tbl_state["bra_id"].str.slice(0, 3)
    tbl_state = tbl_state.groupby(this_pk).agg(agg_rules)

    print "Step D."
    tbl_meso = tbl.reset_index()
    tbl_meso["bra_id"] = tbl_meso["bra_id"].str.slice(0, 5)
    tbl_meso = tbl_meso.groupby(this_pk).agg(agg_rules)

    print "Step E."
    tbl_micro = tbl.reset_index()
    tbl_micro["bra_id"] = tbl_micro["bra_id"].str.slice(0, 7)
    tbl_micro = tbl_micro.groupby(this_pk).agg(agg_rules)

    print "Step F."
    tbl_pr = tbl.reset_index()
    tbl_pr = tbl_pr[tbl_pr["bra_id"].map(lambda x: x[:3] == "4mg")]
    tbl_pr["bra_id"] = tbl_pr["bra_id"].astype(str).replace(planning_regions)
    tbl_pr = tbl_pr.groupby(this_pk).agg(agg_rules)


    master_table = pd.concat([tbl_all, tbl_state, tbl_meso, tbl_micro, tbl_pr, tbl_region])

  
    return master_table

