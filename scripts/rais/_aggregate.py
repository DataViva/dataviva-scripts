import sys
import os
import pandas as pd
import numpy as np
import itertools
import MySQLdb


def get_planning_regions():
    ''' Connect to DB '''
    db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"),
                         user=os.environ["DATAVIVA_DB_USER"],
                         passwd=os.environ["DATAVIVA_DB_PW"],
                         db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()

    cursor.execute("select bra_id, pr_id from attrs_bra_pr")
    return {r[0]: r[1] for r in cursor.fetchall()}


def aggregate_demographics(rais_df, bra_depths):
    rais_df['wage_med'] = rais_df['wage']
    rais_df['num_jobs'] = 1

    rais_df = rais_df.rename(columns={"age": "age_med", "literacy": "edu_mode"})
    rais_df = rais_df.drop(["color", "gender", "est_size"], axis=1)

    print "Aggregating demographic tables, from raw data..."
    dtables = ["ybid", "ybod", "ybd", "yod", "yid"]
    dtbls = {}
    for t_name in dtables:
        table = medians_demographics(rais_df, t_name, bra_depths)
        dtbls[t_name] = table
    return dtbls


def aggregate(rais_df, depths):
    bra_depths = depths["bra"]
    cnae_depths = depths["cnae"]
    cbo_depths = depths["cbo"]
    demo_depths = depths["demo"]

    pk = ["year", "bra_id", "cnae_id", "cbo_id"]
    tables = {"ybio": ["year", "bra_id", "cnae_id", "cbo_id"],
              "ybi": ["year", "bra_id", "cnae_id"],
              "ybo": ["year", "bra_id", "cbo_id"],
              "yb": ["year", "bra_id"],
              "yi": ["year", "cnae_id"],
              "yio": ["year", "cnae_id", "cbo_id"],
              "yo": ["year", "cbo_id"]}
    import time
    s = time.time()

    agg_rules = {
        "wage": np.sum,
        "num_jobs": np.sum,
        "num_emp": pd.Series.nunique,
        "est_id": lambda est_id: set.union(set(est_id)),
        "age": np.sum
    }

    rais_df['num_jobs'] = 1
    ybio_raw = rais_df.groupby(pk).agg(agg_rules)

    agg_rules["est_id"] = lambda x: set.union(*list(x))
    agg_rules["num_emp"] = np.sum

    for t_name in tables.keys():
        ss = time.time()
        print "aggregating", t_name
        tbl_pk = tables[t_name]

        if len(pk) != len(tbl_pk):
            tables[t_name] = ybio_raw.groupby(level=tbl_pk).agg(agg_rules)
        else:
            tables[t_name] = ybio_raw.copy()

        if "cbo_id" in tbl_pk:
            # print "cbo"
            ybio_new_depths = pd.DataFrame()
            for depth in cbo_depths[:-1]:
                # print "  ", depth
                ybio_depth = tables[t_name].reset_index()
                ybio_depth["cbo_id"] = ybio_depth["cbo_id"].str.slice(0, depth)
                ybio_depth = ybio_depth.groupby(tbl_pk).agg(agg_rules)
                ybio_new_depths = pd.concat([ybio_new_depths, ybio_depth])
            tables[t_name] = pd.concat([ybio_new_depths, tables[t_name]])

        if "cnae_id" in tbl_pk:
            # print "cnae"
            ybio_new_depths = pd.DataFrame()
            for depth in cnae_depths[:-1]:
                # print "  ", depth
                ybio_depth = tables[t_name].reset_index()
                ybio_depth["cnae_id"] = ybio_depth["cnae_id"].str.slice(0, depth)
                ybio_depth = ybio_depth.groupby(tbl_pk).agg(agg_rules)
                ybio_new_depths = pd.concat([ybio_new_depths, ybio_depth])
            tables[t_name] = pd.concat([ybio_new_depths, tables[t_name]])

        if "bra_id" in tbl_pk:
            # print "bra"
            ybio_new_depths = pd.DataFrame()
            for depth in bra_depths[:-1]:
                # print "  ", depth
                ybio_depth = tables[t_name].reset_index()
                if depth == 8:
                    ybio_depth = ybio_depth[ybio_depth["bra_id"].map(lambda x: x[:3] == "4mg")]
                    ybio_depth["bra_id"] = ybio_depth["bra_id"].astype(str).replace(get_planning_regions())
                else:
                    ybio_depth["bra_id"] = ybio_depth["bra_id"].str.slice(0, depth)
                ybio_depth = ybio_depth.groupby(tbl_pk).agg(agg_rules)
                ybio_new_depths = pd.concat([ybio_new_depths, ybio_depth])
            tables[t_name] = pd.concat([ybio_new_depths, tables[t_name]])

        # tables[t_name]["num_emp"] = tables[t_name]["num_emp"].apply(lambda x: len(x))
        tables[t_name]["est_id"] = tables[t_name]["est_id"].apply(lambda x: len(x))
        tables[t_name]["wage_avg"] = tables[t_name]["wage"] / tables[t_name]["num_jobs"]
        tables[t_name]["age"] = tables[t_name]["age"] / tables[t_name]["num_jobs"]
        tables[t_name] = tables[t_name].rename(columns={"age": "age_avg", "est_id": "num_est"})

        print "Is unique:", tables[t_name].index.is_unique

        # tables[t_name].to_csv(t_name+".csv")
        print "time:", (time.time() - ss) / 60

    print "total aggregation time:", (time.time() - s) / 60

    return tables
