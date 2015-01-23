import sys, MySQLdb, os
import pandas as pd
import numpy as np
import re

agg_rules = {
    "age" : np.mean,
    "enroll_id": pd.Series.count,
    "class_id": pd.Series.nunique,
    "distorted_age" : np.sum,
}

commute_rules = {
    "commute_distance" : np.mean
}

# pk = ["year", "bra_id", "school_id", "course_sc_id", "d_id"]

db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), user=os.environ["DATAVIVA_DB_USER"], passwd=os.environ["DATAVIVA_DB_PW"], db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
distances = {} 

def get_planning_regions():
    ''' Connect to DB '''
    cursor = db.cursor()
    cursor.execute("select bra_id, pr_id from attrs_bra_pr")
    return {r[0]:r[1] for r in cursor.fetchall()}

planning_regions = get_planning_regions()


def aggregate(table_name, this_pk, tbl, dem, cid_len=None, course_flag=None):

    if dem:
        tbl["d_id"] = tbl[dem]
        # this_pk = pk
    # else:
        # this_pk = pk[:-1]
    
    # print "getting distances..."
    if table_name != "yb":
        this_agg_rules = agg_rules
    else:
        this_agg_rules = {
            "age" : np.mean,
            "enroll_id": pd.Series.count,
            "class_id": pd.Series.nunique,
            "distorted_age" : np.sum,
            "school_id" : pd.Series.nunique,
        }


    # tbl = tbl.drop(['gender', 'color', 'loc', 'school_type'], axis=1)
    
    if cid_len:
        tbl['course_sc_id'] = tbl["course_sc_id"].str.slice(0, cid_len)

    print "Step A."
    tbl_all = tbl.groupby(this_pk).agg(this_agg_rules)
    # print tbl_all[tbl_all.commute_distance > 0].head()

    print "Step B."
    tbl_region = tbl.reset_index()
    tbl_region["bra_id"] = tbl_region["bra_id"].str.slice(0, 1)
    tbl_region = tbl_region.groupby(this_pk).agg(this_agg_rules)
    
    print "Step C."
    tbl_state = tbl.reset_index()
    tbl_state["bra_id"] = tbl_state["bra_id"].str.slice(0, 3)
    tbl_state = tbl_state.groupby(this_pk).agg(this_agg_rules)

    print "Step D."
    tbl_meso = tbl.reset_index()
    tbl_meso["bra_id"] = tbl_meso["bra_id"].str.slice(0, 5)
    tbl_meso = tbl_meso.groupby(this_pk).agg(this_agg_rules)

    print "Step E."
    tbl_micro = tbl.reset_index()
    tbl_micro["bra_id"] = tbl_micro["bra_id"].str.slice(0, 7)
    tbl_micro = tbl_micro.groupby(this_pk).agg(this_agg_rules)

    print "Step F."
    tbl_pr = tbl.reset_index()
    tbl_pr = tbl_pr[tbl_pr["bra_id"].map(lambda x: x[:3] == "4mg")]
    tbl_pr["bra_id"] = tbl_pr["bra_id"].astype(str).replace(planning_regions)
    tbl_pr = tbl_pr.groupby(this_pk).agg(this_agg_rules)


    master_table = pd.concat([tbl_all, tbl_state, tbl_meso, tbl_micro, tbl_pr, tbl_region])

    if cid_len or course_flag:
        print "Step G. (course_sc_id step) compute distortion rate"
        master_table['distortion_rate'] = master_table["distorted_age"] / master_table["enroll_id"]
        master_table.loc[master_table['distorted_age'].isnull() , 'distortion_rate'] = '\N'
    
    master_table.drop('distorted_age', axis=1, inplace=True)
    return master_table

