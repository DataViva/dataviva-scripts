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
        'age': np.mean,
        'enroll_id': pd.Series.count,
        'class_id': pd.Series.nunique,
        'distorted_age': np.sum,
    }

    if indexes == ['year', 'bra_id']:
        agg_rules['school_id'] = pd.Series.nunique

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
            aggregated_df[level.column] = aggregated_df[level.column].str.slice(0, level.depth)
            if 'course_sc_id' in indexes:
                aggregated_df['distortion_rate'] = aggregated_df['distorted_age'] / aggregated_df['enroll_id']
                aggregated_df.loc[aggregated_df['distorted_age'].isnull(), 'distortion_rate'] = '\N'
                aggregated_df.drop('distorted_age', axis=1, inplace=True)
        aggregated_dfs.append(aggregated_df.groupby(indexes).agg(agg_rules))

    return pd.concat(aggregated_dfs) if aggregated_dfs else df.groupby(indexes).agg(agg_rules)
