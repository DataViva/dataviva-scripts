import pandas as pd
import numpy as np


def aggregate(this_pk, tbl):
    tbl = tbl.drop(['gender', 'ethnicity', 'school_type'], axis=1)

    # -- For aggregation make sure we are only looking at the deepest level!!
    deepestBra = tbl.bra_id.str.len() == 9
    deepestCourse = tbl.course_hedu_id.str.len() == 6

    agg_rules = {
        "age": np.mean,
        "enrolled": np.sum,
        "entrants": np.sum,
        "graduates": np.sum,
        "student_id": pd.Series.nunique,
        "morning": np.sum,
        "afternoon": np.sum,
        "night": np.sum,
        "full_time": np.sum,
        "entrants": np.sum,
    }

    pk_types = set([type(t) for t in this_pk])
    if pk_types == set([str]) and this_pk == ["year", "bra_id"]:
        agg_rules["university_id"] = pd.Series.nunique

    test = tbl[~(deepestBra & deepestCourse)]
    tbl = tbl[deepestBra & deepestCourse]

    if not test.empty:
        print "ROWS REMOVED! On table", this_pk
        print test.head()
    test = None

    df_municipality = tbl.groupby(this_pk).agg(agg_rules)

    df_region = tbl.reset_index()
    df_region["bra_id"] = df_region["bra_id"].str.slice(0, 1)
    df_region = df_region.groupby(this_pk).agg(agg_rules)

    df_state = tbl.reset_index()
    df_state["bra_id"] = df_state["bra_id"].str.slice(0, 3)
    df_state = df_state.groupby(this_pk).agg(agg_rules)

    df_meso = tbl.reset_index()
    df_meso["bra_id"] = df_meso["bra_id"].str.slice(0, 5)
    df_meso = df_meso.groupby(this_pk).agg(agg_rules)

    df_micro = tbl.reset_index()
    df_micro["bra_id"] = df_micro["bra_id"].str.slice(0, 7)
    df_micro = df_micro.groupby(this_pk).agg(agg_rules)

    return pd.concat([df_municipality, df_state, df_meso, df_micro, df_region])
