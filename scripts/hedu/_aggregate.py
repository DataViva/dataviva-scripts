import sys, MySQLdb, os
import pandas as pd
import numpy as np


def get_planning_regions():
    ''' Connect to DB '''
    db = MySQLdb.connect(host=os.environ.get("DATAVIVA2_DB_HOST", "localhost"), user=os.environ["DATAVIVA2_DB_USER"], passwd=os.environ["DATAVIVA2_DB_PW"], db=os.environ["DATAVIVA2_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    cursor.execute("select bra_id, pr_id from attrs_bra_pr")
    return {r[0]:r[1] for r in cursor.fetchall()}

def aggregate(this_pk, tbl, dem):
    if dem:
        tbl["d_id"] = tbl[dem]
    
    tbl = tbl.drop(['gender', 'ethnicity', 'school_type'], axis=1)

    # -- For aggregation make sure we are only looking at the deepest level!!
    deepestBra = tbl.bra_id.str.len() == 9
    deepestCourse = tbl.course_hedu_id.str.len() == 6

    agg_rules = {
        "age" : np.mean,
        "enrolled": np.sum,
        "entrants": np.sum,
        "graduates": np.sum,
        "student_id": pd.Series.nunique,
        "morning": np.sum,
        "afternoon" : np.sum,
        "night": np.sum,
        "full_time": np.sum,
        "entrants": np.sum,
    }
    
    pk_types = set([type(t) for t in this_pk])
    if pk_types == set([str]) and this_pk == ["year", "bra_id"]:
        agg_rules["university_id"] = pd.Series.nunique

    test = tbl[ ~(deepestBra & deepestCourse) ]
    tbl = tbl[ deepestBra & deepestCourse ]

    if not test.empty:
        print "ROWS REMOVED! On table", this_pk 
        print test.head()
    test = None

    tbl_all = tbl.groupby(this_pk).agg(agg_rules)

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

    tbl_pr = tbl.reset_index()
    tbl_pr = tbl_pr[tbl_pr["bra_id"].map(lambda x: x[:3] == "4mg")]
    tbl_pr["bra_id"] = tbl_pr["bra_id"].astype(str).replace(get_planning_regions())
    tbl_pr = tbl_pr.groupby(this_pk).agg(agg_rules)

    return pd.concat([tbl_all, tbl_state, tbl_meso, tbl_micro, tbl_pr, tbl_region])    
