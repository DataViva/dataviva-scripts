import pandas as pd
import numpy as np


def aggregate(table_name, indexes, df):
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

    # if cid_len or course_flag:
    #    print "Step G. (course_sc_id step) compute distortion rate"
    #    df['distortion_rate'] = df["distorted_age"] / df["enroll_id"]
    #    df.loc[df['distorted_age'].isnull() , 'distortion_rate'] = '\N'
    # df.drop('distorted_age', axis=1, inplace=True)

    if 'course_sc_id' in indexes:
        df_fields = df.reset_index()
        df_fields["course_sc_id"] = df_fields["course_sc_id"].str.slice(0, 2)
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
