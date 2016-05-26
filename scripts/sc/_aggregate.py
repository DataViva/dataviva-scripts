import pandas as pd
import numpy as np
import itertools
from collections import namedtuple


def aggregate(indexes, df):
    Level = namedtuple('Level', ['column', 'depth'])

    depths = {
        'bra_id': [
            Level('bra_id', 1),
            Level('bra_id', 3),
            Level('bra_id', 5),
            Level('bra_id', 7),
            Level('bra_id', 9),
        ],

        'course_sc_id': [
            Level('course_sc_id', 2),
            Level('course_sc_id', 5),
        ],
    }

    agg_rules = {
        "age": np.mean,
        "enroll_id": pd.Series.count,
        "class_id": pd.Series.nunique,
        "distorted_age": np.sum,
    }

    if indexes == ["year", "bra_id"]:
        agg_rules["school_id"] = pd.Series.nunique

    aggregated_dfs = []
    aggregated_dfs.append(df.groupby(indexes).agg(agg_rules))

    index_depths = list(set(indexes) & set(depths.keys()))

    aggregation_levels = list(itertools.product(
        *[[level for level in depths[index]] for index in index_depths]
    ))

    aggregated_dfs = []

    for levels in aggregation_levels:
        aggregated_df = df.reset_index()
        for level in levels:
            aggregated_df[level.column] = aggregated_df[
                level.column].str.slice(0, level.depth)
        aggregated_dfs.append(aggregated_df.groupby(indexes).agg(agg_rules))

    return pd.concat(aggregated_dfs) if aggregated_dfs else df.groupby(indexes).agg(agg_rules)

    # if cid_len or course_flag:
    #    print "Step G. (course_sc_id step) compute distortion rate"
    #    df['distortion_rate'] = df["distorted_age"] / df["enroll_id"]
    #    df.loc[df['distorted_age'].isnull() , 'distortion_rate'] = '\N'
    # df.drop('distorted_age', axis=1, inplace=True)
