import pandas as pd
import numpy as np
import collections

RECEIVER, SENDER = "receiver", "sender"

def mylen(x):
    return len(x) if isinstance(x, collections.Iterable) else 1

BLACKLISTED = -999

def setup(datapath, blpath):
    df = pd.read_csv(datapath, sep=";")
    blacklist_df = pd.read_csv(blpath, sep=";")


    df = pd.merge(df, blacklist_df, how="inner", left_on=["bra_r", "cnae_r"], right_on=["ID_Municipality", "EconomicAtivity_ID_CNAE_2d"])
    df = pd.merge(df, blacklist_df, how="inner", left_on=["bra_s", "cnae_s"], right_on=["ID_Municipality", "EconomicAtivity_ID_CNAE_2d"], suffixes=["_r", "_s"])

    df["cnae_r_og"] = df.cnae_r
    df["cnae_s_og"] = df.cnae_s

    blacklist_df = blacklist_df.set_index(["ID_Municipality", "EconomicAtivity_ID_CNAE_2d"])
    
    def uniq_ests(bra, cnae):
        if not isinstance(cnae, collections.Iterable):
            cnae = [cnae]
        total = 0
        for x in cnae:
            total += blacklist_df.xs([bra, x]).Establishments_number
        return total

    # df.value = df.value.str.replace(",", ".").astype(float)

    df.loc[ df.bl_s == 1, 'cnae_s'] = BLACKLISTED
    df.loc[ df.bl_r == 1, 'cnae_r'] = BLACKLISTED

    df = df.drop(labels=["bl_s", "bl_r", "EconomicAtivity_ID_CNAE_2d_r", "ID_Municipality_s", "EconomicAtivity_ID_CNAE_2d_r", "ID_Municipality_s", "ID_Municipality_s"], axis=1)

    grouped_df = df.groupby(["bra_r", "cnae_r", "bra_s", "cnae_s", "hs_id"]).agg({
                "cnae_r_og": lambda x: set.union(set(x)),
                "cnae_s_og" : lambda x: set.union(set(x)),
                "value" : pd.Series.sum,
    })

    grouped_df = grouped_df.reset_index()

    grouped_df['num_est_r'] = grouped_df.apply(lambda x: uniq_ests(x["bra_r"], x["cnae_r_og"]), axis=1)
    grouped_df['num_est_s'] = grouped_df.apply(lambda x: uniq_ests(x["bra_s"], x["cnae_s_og"]), axis=1)
    grouped_df["num_cnae_r"] = grouped_df.cnae_r_og.map(mylen)
    grouped_df["num_cnae_s"] = grouped_df.cnae_s_og.map(mylen)

    return grouped_df, blacklist_df

def rule1(grouped_df, blacklist_df, mode):
    # # -- Now work on RECEIVERS, TODO: work on senders
    if mode == RECEIVER:
        bad_df_cond = (grouped_df.cnae_r == BLACKLISTED) & (grouped_df.num_est_r < 3)
        agg_pk = ["bra_r", "bra_s", "cnae_s", "hs_id"]
        cnae_lbl = 'cnae_r'
        cnae_other='cnae_s'
    else:
        bad_df_cond = (grouped_df.cnae_s == BLACKLISTED) & (grouped_df.num_est_s < 3)
        agg_pk = ["bra_r", "cnae_r", "bra_s", "hs_id"]
        cnae_lbl = 'cnae_s'
        cnae_other = 'cnae_r'

    bad_df = grouped_df[ bad_df_cond ] 

    # count the number of violations!
    violations_count = bad_df.bra_r.count()
    print "THERE ARE %s rule 1 violations with MODE=%s" % (violations_count, mode)
    if not violations_count:
        print "FOUND no violations, no updates needed"
        return grouped_df

    if mode == RECEIVER:
        good_df_cond = (grouped_df.bra_r.isin(bad_df.bra_r)) & (grouped_df.cnae_r != BLACKLISTED)
    else:
        good_df_cond = (grouped_df.bra_s.isin(bad_df.bra_s)) & (grouped_df.cnae_s != BLACKLISTED)

    # good df stores the minimum values
    good_df = grouped_df[ good_df_cond ]
    good_df = grouped_df.iloc[ good_df.groupby(agg_pk).agg({"value":pd.Series.idxmin}).value ]

    bad_df = pd.merge(bad_df, good_df, how='left', left_on=agg_pk, right_on=agg_pk, suffixes=["_bad", "_good"])
    bad_df = bad_df[bad_df.value_good.notnull()]
    bad_df['value'] = bad_df.value_bad + bad_df.value_good
    if mode == RECEIVER:
        bad_df['num_est_r'] = bad_df.num_est_r_bad + bad_df.num_est_r_good
        bad_df['num_cnae_r'] = bad_df.num_cnae_r_bad + 1
        bad_df['num_cnae_s'] = bad_df.num_cnae_s_bad # -- Don't add here because this is receiver mode
        bad_df['num_est_s'] = bad_df.num_est_s_bad # -- Don't add here
    else:
        bad_df['num_est_s'] = bad_df.num_est_s_bad + bad_df.num_est_s_good
        bad_df['num_cnae_s'] = bad_df.num_cnae_s_bad + 1
        bad_df['num_cnae_r'] = bad_df.num_cnae_r_bad # -- Don't add here because this is receiver mode
        bad_df['num_est_r'] = bad_df.num_est_r_bad # -- Don't add here
    bad_df[cnae_lbl] = BLACKLISTED

    bad_df['cnae_r_og'] = bad_df.apply(lambda x: x["cnae_r_og_bad"].union(x["cnae_r_og_good"]), axis=1)
    bad_df['cnae_s_og'] = bad_df.apply(lambda x: x["cnae_s_og_bad"].union(x["cnae_s_og_good"]), axis=1)

    # DROP ROWS from master table
    to_remove = bad_df[["bra_r", cnae_lbl + "_good", "bra_s", cnae_other, "hs_id"]]
    to_remove = to_remove.rename(columns={cnae_lbl+"_good": cnae_lbl})
    to_remove["delete"] = 1
    grouped_df = pd.merge(grouped_df, to_remove, how="left", left_on=["bra_r", "cnae_r", "bra_s", "cnae_s", "hs_id"], right_on=["bra_r", "cnae_r", "bra_s", "cnae_s", "hs_id"])
    grouped_df = grouped_df.loc[grouped_df.delete != 1]
    del grouped_df["delete"]

    # UPDATE values in master table
    bad_df = bad_df[["bra_r", "cnae_r", "bra_s", "cnae_s", "hs_id", "value", "num_est_r", "num_cnae_r", "num_est_s", "num_cnae_s", "cnae_r_og", "cnae_s_og"]]
    bad_df["update_row"] = 1

    grouped_df = pd.merge(grouped_df, bad_df, how="left", left_on=["bra_r", "cnae_r", "bra_s", "cnae_s", "hs_id"], right_on=["bra_r", "cnae_r", "bra_s", "cnae_s", "hs_id"], suffixes=["_orig", "_fixed"])

    grouped_df.loc[grouped_df.update_row != 1, 'value'] = grouped_df.value_orig
    grouped_df.loc[grouped_df.update_row == 1, 'value'] = grouped_df.value_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'num_cnae_r'] = grouped_df.num_cnae_r_orig
    grouped_df.loc[grouped_df.update_row == 1, 'num_cnae_r'] = grouped_df.num_cnae_r_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'num_est_r'] = grouped_df.num_est_r_orig
    grouped_df.loc[grouped_df.update_row == 1, 'num_est_r'] = grouped_df.num_est_r_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'num_est_s'] = grouped_df.num_est_s_orig
    grouped_df.loc[grouped_df.update_row == 1, 'num_est_s'] = grouped_df.num_est_s_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'num_cnae_s'] = grouped_df.num_cnae_s_orig
    grouped_df.loc[grouped_df.update_row == 1, 'num_cnae_s'] = grouped_df.num_cnae_s_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'cnae_r_og'] = grouped_df.cnae_r_og_orig
    grouped_df.loc[grouped_df.update_row == 1, 'cnae_r_og'] = grouped_df.cnae_r_og_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'cnae_s_og'] = grouped_df.cnae_s_og_orig
    grouped_df.loc[grouped_df.update_row == 1, 'cnae_s_og'] = grouped_df.cnae_s_og_fixed

    grouped_df = grouped_df[["bra_r", "cnae_r", "bra_s", "cnae_s", "hs_id", "cnae_s_og", "cnae_r_og", "value", "num_cnae_r", "num_est_r", "num_est_s","num_cnae_s"]]
    return grouped_df


###########################
# RULE 2                  #
###########################

def rule2(grouped_df):
    print grouped_df
    grouped_df['total_cnaes'] = grouped_df.apply(lambda x: len( set(x["cnae_r_og"].union(x["cnae_s_og"])) ), axis=1)
    grouped_df['hs_id_og'] = grouped_df['hs_id']
    grouped_df.loc[ ( (grouped_df.cnae_r == BLACKLISTED) | (grouped_df.cnae_s == BLACKLISTED) ) & (grouped_df.total_cnaes < 3) , 'hs_id' ] = BLACKLISTED

    grouped_df = grouped_df.groupby(["bra_r", "cnae_r", "bra_s", "cnae_s", "hs_id"]).agg({
            "cnae_s_og" : lambda x: set.union(*list(x)),
            "cnae_r_og" : lambda x: set.union(*list(x)),
            "value" : pd.Series.sum,
            "num_cnae_r": pd.Series.sum,
            "num_cnae_s" : pd.Series.sum,
            "hs_id_og": lambda x: set.union(set(x)),
    }).reset_index()

    # '''
    # If, at this point, any of those other hs_id rows 
    # has less than 3 num_cnae, merge in the smallest data 
    # rows that match the 4 other indices for that row 
    # (bra_s, cnae_s, bra_r, cnae_r) and change those small 
    # hs_ids to other
    # '''
    # agg_pk = ["bra_s", "cnae_s", "bra_r", "cnae_r"]
    grouped_df["hs_set_len"] = grouped_df.hs_id_og.apply(lambda x: len(x))
    bad_df = grouped_df[(grouped_df.hs_id == BLACKLISTED) & ( grouped_df.hs_set_len < 3)  ]

    bad_count = bad_df.bra_r.count()
    print " !!! WARNING: there are %s products on the blacklist with less than 3 products" % (bad_count)
    print bad_df.head()
    # good_df = grouped_df.iloc[ grouped_df[(grouped_df.hs_id != BLACKLISTED)].groupby(agg_pk).agg({"value":pd.Series.idxmin}).value ]

    # violations = bad_df.hs_id.count()
    # print "Step #3: number of violations", violations
    # if violations:
    #     pass
    return grouped_df


mygrouped_df, blacklist_df = setup("rule2/small_2013_01.csv", "rule2/BlackList_2013_01.csv")
mygrouped_df = rule1(mygrouped_df, blacklist_df, RECEIVER)
mygrouped_df = rule1(mygrouped_df, blacklist_df, SENDER)
mygrouped_df = rule2(mygrouped_df)
print mygrouped_df
# print grouped_df
