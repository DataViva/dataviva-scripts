import pandas as pd
import numpy as np
import collections

RECEIVER, SENDER = "receiver", "sender"
CNAE_BLACKLISTED = 'x01'
HS_BLACKLISTED = 'XX0023'

def mylen(x):
    return len(x) if isinstance(x, collections.Iterable) else 1

def setup(datapath, blpath):
    df = pd.read_csv(datapath, sep=";")
    blacklist_df = pd.read_csv(blpath, sep=";")

    
    df = pd.merge(df, blacklist_df, how="inner", left_on=["bra_id_r", "cnae_id_r"], right_on=["ID_Municipality", "EconomicAtivity_ID_CNAE_2d"])
    df = pd.merge(df, blacklist_df, how="inner", left_on=["bra_id_s", "cnae_id_s"], right_on=["ID_Municipality", "EconomicAtivity_ID_CNAE_2d"], suffixes=["_r", "_s"])

    df["cnae_r_og"] = df.cnae_id_r
    df["cnae_s_og"] = df.cnae_id_s

    blacklist_df = blacklist_df.set_index(["ID_Municipality", "EconomicAtivity_ID_CNAE_2d"])
    
    def uniq_ests(bra, cnae):
        if not isinstance(cnae, collections.Iterable):
            cnae = [cnae]
        total = 0
        for x in cnae:
            total += blacklist_df.xs([bra, x]).Establishments_number
        return total

    # df.value = df.value.str.replace(",", ".").astype(float)

    df.loc[ df.bl_s == 1, 'cnae_id_s'] = CNAE_BLACKLISTED
    df.loc[ df.bl_r == 1, 'cnae_id_r'] = CNAE_BLACKLISTED

    df = df.drop(labels=["bl_s", "bl_r", "EconomicAtivity_ID_CNAE_2d_r", "ID_Municipality_s", "EconomicAtivity_ID_CNAE_2d_r", "ID_Municipality_s", "ID_Municipality_s"], axis=1)

    grouped_df = df.groupby(["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"]).agg({
                "cnae_r_og": lambda x: set.union(set(x)),
                "cnae_s_og" : lambda x: set.union(set(x)),
                "value" : pd.Series.sum,
    })

    grouped_df = grouped_df.reset_index()

    grouped_df['num_est_r'] = grouped_df.apply(lambda x: uniq_ests(x["bra_id_r"], x["cnae_r_og"]), axis=1)
    grouped_df['num_est_s'] = grouped_df.apply(lambda x: uniq_ests(x["bra_id_s"], x["cnae_s_og"]), axis=1)
    grouped_df["num_cnae_r"] = grouped_df.cnae_r_og.map(mylen)
    grouped_df["num_cnae_s"] = grouped_df.cnae_s_og.map(mylen)

    return grouped_df, blacklist_df


def rule1(grouped_df, blacklist_df, mode):
    # # -- Now work on RECEIVERS, TODO: work on senders
    if mode == RECEIVER:
        bad_df_cond = (grouped_df.cnae_id_r == CNAE_BLACKLISTED) & (grouped_df.num_est_r < 3)
        agg_pk = ["bra_id_r", "bra_id_s", "cnae_id_s", "hs_id"]
        cnae_lbl = 'cnae_id_r'
        cnae_other='cnae_id_s'
    else:
        bad_df_cond = (grouped_df.cnae_id_s == CNAE_BLACKLISTED) & (grouped_df.num_est_s < 3)
        agg_pk = ["bra_id_r", "cnae_id_r", "bra_id_s", "hs_id"]
        cnae_lbl = 'cnae_id_s'
        cnae_other = 'cnae_id_r'

    bad_df = grouped_df[ bad_df_cond ] 

    # count the number of violations!
    violations_count = bad_df.bra_id_r.count()
    print "THERE ARE %s rule 1 violations with MODE=%s" % (violations_count, mode)
    if not violations_count:
        print "FOUND no violations, no updates needed"
        return grouped_df

    if mode == RECEIVER:
        good_df_cond = (grouped_df.bra_id_r.isin(bad_df.bra_id_r)) & (grouped_df.cnae_id_r != CNAE_BLACKLISTED)
    else:
        good_df_cond = (grouped_df.bra_id_s.isin(bad_df.bra_id_s)) & (grouped_df.cnae_id_s != CNAE_BLACKLISTED)



    # good df stores the minimum values
    good_df = grouped_df[ good_df_cond ]
    good_df = grouped_df.iloc[ good_df.groupby(agg_pk).agg({"value": pd.Series.idxmin}).value ]
    # print good_df.head(), "GOOD"

    bad_df = pd.merge(bad_df, good_df, how='left', left_on=agg_pk, right_on=agg_pk, suffixes=["_bad", "_good"])
    bad_df = bad_df[bad_df.value_good.notnull()]

    bad_df['value'] = bad_df.value_bad + bad_df.value_good
    # bad_df['transfer_value'] = bad_df.transfer_value_bad + bad_df.transfer_value_good
    # bad_df['devolution_value'] = bad_df.devolution_value_bad + bad_df.devolution_value_good
    # bad_df['icms_credit_value'] = bad_df.icms_credit_value_bad + bad_df.icms_credit_value_good
    # bad_df['icms_tax'] = bad_df.icms_tax_bad + bad_df.icms_tax_good
    # bad_df['tax'] = bad_df.tax_bad + bad_df.tax_good

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

    bad_df[cnae_lbl] = CNAE_BLACKLISTED
    # print bad_df.cnae_r_og_bad

    # bad_df['cnae_r_og'] = bad_df.apply(join_r_sets, axis=1)
    # bad_df['cnae_s_og'] = bad_df.apply(join_s_sets, axis=1)
    bad_df['cnae_r_og'] = bad_df.apply(lambda x: x["cnae_r_og_bad"].union(x["cnae_r_og_good"]), axis=1)
    bad_df['cnae_s_og'] = bad_df.apply(lambda x: x["cnae_s_og_bad"].union(x["cnae_s_og_good"]), axis=1)
    # import sys; sys.exit(-1)
    # DROP ROWS from master table
    to_remove = bad_df[["bra_id_r", cnae_lbl + "_good", "bra_id_s", cnae_other, "hs_id"]]
    to_remove = to_remove.rename(columns={cnae_lbl+"_good": cnae_lbl})
    to_remove["delete"] = 1
    grouped_df = pd.merge(grouped_df, to_remove, how="left", left_on=["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"], right_on=["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"])
    grouped_df = grouped_df.loc[grouped_df.delete != 1]
    del grouped_df["delete"]

    # UPDATE values in master table
    bad_df = bad_df[["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id", "value", "num_est_r", "num_cnae_r", "num_est_s", "num_cnae_s", "cnae_r_og", "cnae_s_og"]]
    bad_df["update_row"] = 1

    grouped_df = pd.merge(grouped_df, bad_df, how="left", left_on=["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"], right_on=["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"], suffixes=["_orig", "_fixed"])

    grouped_df.loc[grouped_df.update_row != 1, 'value'] = grouped_df.value_orig
    grouped_df.loc[grouped_df.update_row == 1, 'value'] = grouped_df.value_fixed
    # grouped_df.loc[grouped_df.update_row != 1, 'transfer_value'] = grouped_df.purchase_value_orig
    # grouped_df.loc[grouped_df.update_row == 1, 'transfer_value'] = grouped_df.purchase_value_fixed
    # grouped_df.loc[grouped_df.update_row != 1, 'devolution_value'] = grouped_df.devolution_value_orig
    # grouped_df.loc[grouped_df.update_row == 1, 'devolution_value'] = grouped_df.devolution_value_fixed
    # grouped_df.loc[grouped_df.update_row != 1, 'icms_credit_value'] = grouped_df.icms_credit_value_orig
    # grouped_df.loc[grouped_df.update_row == 1, 'icms_credit_value'] = grouped_df.icms_credit_value_fixed
    # grouped_df.loc[grouped_df.update_row != 1, 'icms_tax'] = grouped_df.icms_tax_orig
    # grouped_df.loc[grouped_df.update_row == 1, 'icms_tax'] = grouped_df.icms_tax_fixed
    # grouped_df.loc[grouped_df.update_row != 1, 'tax'] = grouped_df.tax_orig
    # grouped_df.loc[grouped_df.update_row == 1, 'tax'] = grouped_df.tax_fixed

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

    grouped_df = grouped_df[["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id", "cnae_s_og", "cnae_r_og", "value", "num_cnae_r", "num_est_r", "num_est_s","num_cnae_s"]]
    
    bad_df = grouped_df[ bad_df_cond ] 

    # count the number of violations!
    violations_count = bad_df.bra_id_r.count()
    print "THERE ARE %s rule 1 violations with MODE=%s" % (violations_count, mode)
    if not violations_count:
        print "FOUND no violations, no updates needed"
        return grouped_df

    return grouped_df

# def rule1(grouped_df, blacklist_df, mode):
#     # # -- Now work on RECEIVERS, TODO: work on senders
#     if mode == RECEIVER:
#         bad_df_cond = (grouped_df.cnae_r == BLACKLISTED) & (grouped_df.num_est_r < 3)
#         agg_pk = ["bra_id_r", "bra_s", "cnae_s", "hs_id"]
#         cnae_lbl = 'cnae_r'
#         cnae_other='cnae_s'
#     else:
#         bad_df_cond = (grouped_df.cnae_s == BLACKLISTED) & (grouped_df.num_est_s < 3)
#         agg_pk = ["bra_id_r", "cnae_r", "bra_s", "hs_id"]
#         cnae_lbl = 'cnae_s'
#         cnae_other = 'cnae_r'

#     bad_df = grouped_df[ bad_df_cond ] 

#     # count the number of violations!
#     violations_count = bad_df.bra_id_r.count()
#     print "THERE ARE %s rule 1 violations with MODE=%s" % (violations_count, mode)
#     if not violations_count:
#         print "FOUND no violations, no updates needed"
#         return grouped_df

#     if mode == RECEIVER:
#         good_df_cond = (grouped_df.bra_id_r.isin(bad_df.bra_id_r)) & (grouped_df.cnae_r != BLACKLISTED)
#     else:
#         good_df_cond = (grouped_df.bra_s.isin(bad_df.bra_s)) & (grouped_df.cnae_s != BLACKLISTED)

#     # good df stores the minimum values
#     good_df = grouped_df[ good_df_cond ]
#     good_df = grouped_df.iloc[ good_df.groupby(agg_pk).agg({"value":pd.Series.idxmin}).value ]

#     bad_df = pd.merge(bad_df, good_df, how='left', left_on=agg_pk, right_on=agg_pk, suffixes=["_bad", "_good"])
#     bad_df = bad_df[bad_df.value_good.notnull()]
#     bad_df['value'] = bad_df.value_bad + bad_df.value_good
#     if mode == RECEIVER:
#         bad_df['num_est_r'] = bad_df.num_est_r_bad + bad_df.num_est_r_good
#         bad_df['num_cnae_r'] = bad_df.num_cnae_r_bad + 1
#         bad_df['num_cnae_s'] = bad_df.num_cnae_s_bad # -- Don't add here because this is receiver mode
#         bad_df['num_est_s'] = bad_df.num_est_s_bad # -- Don't add here
#     else:
#         bad_df['num_est_s'] = bad_df.num_est_s_bad + bad_df.num_est_s_good
#         bad_df['num_cnae_s'] = bad_df.num_cnae_s_bad + 1
#         bad_df['num_cnae_r'] = bad_df.num_cnae_r_bad # -- Don't add here because this is receiver mode
#         bad_df['num_est_r'] = bad_df.num_est_r_bad # -- Don't add here
#     bad_df[cnae_lbl] = BLACKLISTED

#     bad_df['cnae_r_og'] = bad_df.apply(lambda x: x["cnae_r_og_bad"].union(x["cnae_r_og_good"]), axis=1)
#     bad_df['cnae_s_og'] = bad_df.apply(lambda x: x["cnae_s_og_bad"].union(x["cnae_s_og_good"]), axis=1)

#     # DROP ROWS from master table
#     to_remove = bad_df[["bra_id_r", cnae_lbl + "_good", "bra_s", cnae_other, "hs_id"]]
#     to_remove = to_remove.rename(columns={cnae_lbl+"_good": cnae_lbl})
#     to_remove["delete"] = 1
#     grouped_df = pd.merge(grouped_df, to_remove, how="left", left_on=["bra_id_r", "cnae_r", "bra_s", "cnae_s", "hs_id"], right_on=["bra_id_r", "cnae_r", "bra_s", "cnae_s", "hs_id"])
#     grouped_df = grouped_df.loc[grouped_df.delete != 1]
#     del grouped_df["delete"]

#     # UPDATE values in master table
#     bad_df = bad_df[["bra_id_r", "cnae_r", "bra_s", "cnae_s", "hs_id", "value", "num_est_r", "num_cnae_r", "num_est_s", "num_cnae_s", "cnae_r_og", "cnae_s_og"]]
#     bad_df["update_row"] = 1

#     grouped_df = pd.merge(grouped_df, bad_df, how="left", left_on=["bra_id_r", "cnae_r", "bra_s", "cnae_s", "hs_id"], right_on=["bra_id_r", "cnae_r", "bra_s", "cnae_s", "hs_id"], suffixes=["_orig", "_fixed"])

#     grouped_df.loc[grouped_df.update_row != 1, 'value'] = grouped_df.value_orig
#     grouped_df.loc[grouped_df.update_row == 1, 'value'] = grouped_df.value_fixed
#     grouped_df.loc[grouped_df.update_row != 1, 'num_cnae_r'] = grouped_df.num_cnae_r_orig
#     grouped_df.loc[grouped_df.update_row == 1, 'num_cnae_r'] = grouped_df.num_cnae_r_fixed
#     grouped_df.loc[grouped_df.update_row != 1, 'num_est_r'] = grouped_df.num_est_r_orig
#     grouped_df.loc[grouped_df.update_row == 1, 'num_est_r'] = grouped_df.num_est_r_fixed
#     grouped_df.loc[grouped_df.update_row != 1, 'num_est_s'] = grouped_df.num_est_s_orig
#     grouped_df.loc[grouped_df.update_row == 1, 'num_est_s'] = grouped_df.num_est_s_fixed
#     grouped_df.loc[grouped_df.update_row != 1, 'num_cnae_s'] = grouped_df.num_cnae_s_orig
#     grouped_df.loc[grouped_df.update_row == 1, 'num_cnae_s'] = grouped_df.num_cnae_s_fixed
#     grouped_df.loc[grouped_df.update_row != 1, 'cnae_r_og'] = grouped_df.cnae_r_og_orig
#     grouped_df.loc[grouped_df.update_row == 1, 'cnae_r_og'] = grouped_df.cnae_r_og_fixed
#     grouped_df.loc[grouped_df.update_row != 1, 'cnae_s_og'] = grouped_df.cnae_s_og_orig
#     grouped_df.loc[grouped_df.update_row == 1, 'cnae_s_og'] = grouped_df.cnae_s_og_fixed

#     grouped_df = grouped_df[["bra_id_r", "cnae_r", "bra_s", "cnae_s", "hs_id", "cnae_s_og", "cnae_r_og", "value", "num_cnae_r", "num_est_r", "num_est_s","num_cnae_s"]]
#     return grouped_df

mygrouped_df, blacklist_df = setup("rule1/small_2013_01.csv", "rule1/BlackList_2013_01.csv")
# mygrouped_df = rule1(mygrouped_df, blacklist_df, RECEIVER)
# mygrouped_df = rule1(mygrouped_df, blacklist_df, SENDER)
# mygrouped_df = rule1(mygrouped_df, blacklist_df, SENDER)
# mygrouped_df = rule1(mygrouped_df, blacklist_df, RECEIVER)

print mygrouped_df