import pandas as pd
import numpy as np


def aggregate(table_name, indexes, df):
    df = df.drop(['gender', 'color', 'loc', 'school_type'], axis=1)

    agg_rules = {
        "age": np.mean,
        "enroll_id": pd.Series.count,
        "class_id": pd.Series.nunique,
        "distorted_age": np.sum,
    }

    pk_types = set([type(t) for t in indexes])
    if pk_types == set([str]) and indexes == ["year", "bra_id"]:
        agg_rules["school_id"] = pd.Series.nunique

    aggregated_dfs = []
    aggregated_dfs.append(df.groupby(indexes).agg(agg_rules))

    df_region = df.reset_index()
    df_region["bra_id"] = df_region["bra_id"].str.slice(0, 1)
    df_region = df_region.groupby(indexes).agg(agg_rules)

    df_state = df.reset_index()
    df_state["bra_id"] = df_state["bra_id"].str.slice(0, 3)
    df_state = df_state.groupby(indexes).agg(agg_rules)

    df_meso = df.reset_index()
    df_meso["bra_id"] = df_meso["bra_id"].str.slice(0, 5)
    df_meso = df_meso.groupby(indexes).agg(agg_rules)

    df_micro = df.reset_index()
    df_micro["bra_id"] = df_micro["bra_id"].str.slice(0, 7)
    df_micro = df_micro.groupby(indexes).agg(agg_rules)

    return pd.concat(aggregated_dfs)
