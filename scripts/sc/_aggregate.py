import pandas as pd
import numpy as np


def aggregate(table_name, this_pk, tbl, course_flag=None):
    tbl = tbl.drop(['gender', 'color', 'loc', 'school_type'], axis=1)

    agg_rules = {
        "age": np.mean,
        "enroll_id": pd.Series.count,
        "class_id": pd.Series.nunique,
        "distorted_age": np.sum,
    }

    pk_types = set([type(t) for t in this_pk])
    if pk_types == set([str]) and this_pk == ["year", "bra_id"]:
        agg_rules["school_id"] = pd.Series.nunique

    tbl_all = tbl.groupby(this_pk).agg(agg_rules)
    # print tbl_all[tbl_all.commute_distance > 0].head()

    tbl_region = tbl.reset_index()
    tbl_region["bra_id"] = tbl_region["bra_id"].str.slice(0, 1)
    tbl_region = tbl_region.groupby(this_pk).agg(agg_rules)

    tbl_state = tbl.reset_index()
    tbl_state["bra_id"] = tbl_state["bra_id"].str.slice(0, 3)
    tbl_state = tbl_state.groupby(this_pk).agg(agg_rules)

    tbl_meso = tbl.reset_index()
    tbl_meso["bra_id"] = tbl_meso["bra_id"].str.slice(0, 5)
    tbl_meso = tbl_meso.groupby(this_pk).agg(agg_rules)

    tbl_micro = tbl.reset_index()
    tbl_micro["bra_id"] = tbl_micro["bra_id"].str.slice(0, 7)
    tbl_micro = tbl_micro.groupby(this_pk).agg(agg_rules)

    master_table = pd.concat([tbl_all, tbl_state, tbl_meso, tbl_micro, tbl_region])

    if course_flag:
        print "Step G. (course_sc_id step) compute distortion rate"
        master_table['distortion_rate'] = master_table["distorted_age"] / master_table["enroll_id"]
        master_table.loc[master_table['distorted_age'].isnull(), 'distortion_rate'] = '\N'

    master_table.drop('distorted_age', axis=1, inplace=True)
    return master_table
