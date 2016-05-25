import pandas as pd
import numpy as np
import itertools
from collections import namedtuple


def aggregate(indexes, df):
    df = df.drop(['gender', 'ethnicity', 'school_type'], axis=1)

    Level = namedtuple('Level', ['column', 'depth'])

    depths = {
        'bra_id': [
            Level('bra_id', 1),
            Level('bra_id', 3),
            Level('bra_id', 5),
            Level('bra_id', 7),
            Level('bra_id', 9),
        ],

        'course_hedu_id': [
            Level('course_hedu_id', 2),
            Level('course_hedu_id', 6),
        ],
    }

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

    if indexes == ["year", "bra_id"]:
        agg_rules["university_id"] = pd.Series.nunique

    index_depths = list(set(indexes) & set(depths.keys()))

    aggregation_levels = list(itertools.product(
        *[[level for level in depths[index]] for index in index_depths]
    ))

    aggregated_dfs = []

    for levels in aggregation_levels:
        aggregated_df = df.reset_index()

        for level in levels:
            print level.depth, level.column
            aggregated_df[level.column] = aggregated_df[level.column].str.slice(0, level.depth)

        aggregated_dfs.append(aggregated_df.groupby(indexes).agg(agg_rules))

    return pd.concat(aggregated_dfs) if aggregated_dfs else df.groupby(indexes).agg(agg_rules)
