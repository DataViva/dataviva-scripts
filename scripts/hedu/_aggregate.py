import sys, bottleneck, MySQLdb, os
import pandas as pd
import numpy as np

agg_rules = {
    "age" : bottleneck.median,
    "enrolled": np.sum,
    "entrants": np.sum,
    "graduates": np.sum,
    "student_id": pd.Series.nunique
}
pk = ["year", "bra_id", "university_id", "course_id", "d_id"]

def get_planning_regions():
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    cursor.execute("select bra_id, pr_id from attrs_bra_pr")
    return {r[0]:r[1] for r in cursor.fetchall()}

def aggregate(tbl, dem):
    if dem:
        tbl["d_id"] = tbl[dem]
        this_pk = pk
    else:
        this_pk = pk[:-1]
    
    tbl = tbl.drop(['gender', 'ethnicity', 'school_type'], axis=1)
    tbl_all = tbl.groupby(this_pk).agg(agg_rules)
    
    tbl_state = tbl.reset_index()
    tbl_state["bra_id"] = tbl_state["bra_id"].str.slice(0, 2)
    tbl_state = tbl_state.groupby(this_pk).agg(agg_rules)

    tbl_meso = tbl.reset_index()
    tbl_meso["bra_id"] = tbl_meso["bra_id"].str.slice(0, 4)
    tbl_meso = tbl_meso.groupby(this_pk).agg(agg_rules)

    tbl_micro = tbl.reset_index()
    tbl_micro["bra_id"] = tbl_micro["bra_id"].str.slice(0, 6)
    tbl_micro = tbl_micro.groupby(this_pk).agg(agg_rules)

    tbl_pr = tbl.reset_index()
    tbl_pr = tbl_pr[tbl_pr["bra_id"].map(lambda x: x[:2] == "mg")]
    tbl_pr["bra_id"] = tbl_pr["bra_id"].astype(str).replace(get_planning_regions())
    tbl_pr = tbl_pr.groupby(this_pk).agg(agg_rules)

    return pd.concat([tbl_all, tbl_state, tbl_meso, tbl_micro, tbl_pr])    
