import pandas as pd
import numpy as np


def aggregate(indexes, df):
    df = df.drop(['gender', 'ethnicity', 'school_type'], axis=1)

    # -- For aggregation make sure we are only looking at the deepest level!!
    deepestBra = df.bra_id.str.len() == 9
    deepestCourse = df.course_hedu_id.str.len() == 6

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

    pk_types = set([type(t) for t in indexes])
    if pk_types == set([str]) and indexes == ["year", "bra_id"]:
        agg_rules["university_id"] = pd.Series.nunique

    test = df[~(deepestBra & deepestCourse)]
    df = df[deepestBra & deepestCourse]

    if not test.empty:
        print "ROWS REMOVED! On table", indexes
        print test.head()
    test = None

    aggregated_dfs = []
    aggregated_dfs.append(df.groupby(indexes).agg(agg_rules))

    if 'course_hedu_id' in indexes:
        df_fields = df.reset_index()
        df_fields["course_hedu_id"] = df_fields["course_hedu_id"].str.slice(0, 2)
        aggregated_dfs.append(df_fields.groupby(indexes).agg(agg_rules))

    if 'bra_id' in indexes:
        df_region = df.reset_index()
        df_region["bra_id"] = df_region["bra_id"].str.slice(0, 1)
        aggregated_dfs.append(df_region.groupby(indexes).agg(agg_rules))

        df_state = df.reset_index()
        df_state["bra_id"] = df_state["bra_id"].str.slice(0, 3)
        aggregated_dfs.append(df_state.groupby(indexes).agg(agg_rules))

        df_meso = df.reset_index()
        df_meso["bra_id"] = df_meso["bra_id"].str.slice(0, 5)
        aggregated_dfs.append(df_meso.groupby(indexes).agg(agg_rules))

        df_micro = df.reset_index()
        df_micro["bra_id"] = df_micro["bra_id"].str.slice(0, 7)
        aggregated_dfs.append(df_micro.groupby(indexes).agg(agg_rules))

    return pd.concat(aggregated_dfs)
