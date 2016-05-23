import MySQLdb
import os
import pandas as pd
import numpy as np

agg_rules = {
    "age": np.mean,
    "enroll_id": pd.Series.count,
    "class_id": pd.Series.nunique,
    "distorted_age": np.sum,
}

db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), user=os.environ[
                     "DATAVIVA_DB_USER"], passwd=os.environ["DATAVIVA_DB_PW"], db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
distances = {}


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
            "age": np.mean,
            "enroll_id": pd.Series.count,
            "class_id": pd.Series.nunique,
            "distorted_age": np.sum,
            "school_id": pd.Series.nunique,
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

    master_table = pd.concat([tbl_all, tbl_state, tbl_meso, tbl_micro, tbl_region])

    if cid_len or course_flag:
        print "Step G. (course_sc_id step) compute distortion rate"
        master_table['distortion_rate'] = master_table["distorted_age"] / master_table["enroll_id"]
        master_table.loc[master_table['distorted_age'].isnull(), 'distortion_rate'] = '\N'

    master_table.drop('distorted_age', axis=1, inplace=True)
    return master_table
