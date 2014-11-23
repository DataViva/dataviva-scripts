# _blacklist.py
import pandas as pd
import numpy as np

from format_raw_data import lookup_cnae, lookup_location, CNAE_BLACKLISTED, HS_BLACKLIST


def flatten_to_cnae(ei_df, aggcols, aggrules, onlyblacklist=True, onlymg=True):
    flatten_df_r = ei_df[["bra_id_r", "cnae_id_r", "is_blacklisted", "product_value", "hs_id"]]
    flatten_df_r = flatten_df_r.rename(columns={"bra_id_r": "bra_id", "cnae_id_r": "cnae_id"})
    flatten_df_s = ei_df[["bra_id_s", "cnae_id_s", "is_blacklisted", "product_value", "hs_id"]] 
    flatten_df_s = flatten_df_s.rename(columns={"bra_id_s": "bra_id", "cnae_id_s": "cnae_id"})
    if onlyblacklist:
        flatten_df_s = flatten_df_s[flatten_df_s.is_blacklisted > 0]
        flatten_df_r = flatten_df_r[flatten_df_r.is_blacklisted > 0]
    if onlymg:
        flatten_df_s = flatten_df_s[flatten_df_s.bra_id.str.startswith("4mg")]
        flatten_df_r = flatten_df_r[flatten_df_r.bra_id.str.startswith("4mg")]
    flatten_df_raw = pd.concat([flatten_df_s, flatten_df_r])
    #{"cnae_id":pd.Series.nunique}
    count_cnaes = flatten_df_raw.groupby(aggcols).agg(aggrules)
    return count_cnaes

def hs_bl_in_city_to_three_products(ei_df, bra_product_df, bra_mode="bra_id_r"):
    print ei_df[ei_df.hs_id == HS_BLACKLIST].ncm.count()
    ei_df = pd.merge(ei_df, bra_product_df, how="left", left_on=[bra_mode, "hs_id"], right_on=["bra_id", "hs_id1"])
    ei_df.loc[ei_df.num_hs.isin([1,2]), 'hs_id'] = HS_BLACKLIST
    print ei_df[ei_df.hs_id == HS_BLACKLIST].ncm.count()
    ei_df = ei_df.drop(labels=["bra_id", "num_hs",  "hs_id1", "is_min",  "hs_id2",  "is_min2"], axis=1)
    ei_df = pd.merge(ei_df, bra_product_df, how="left", left_on=[bra_mode, "hs_id"], right_on=["bra_id", "hs_id2"])
    ei_df.loc[ei_df.num_hs == 1, 'hs_id'] = HS_BLACKLIST
    print ei_df[ei_df.hs_id == HS_BLACKLIST].ncm.count()
    ei_df = ei_df.drop(labels=["bra_id", "num_hs",  "hs_id1", "is_min",  "hs_id2",  "is_min2"], axis=1)
    return ei_df

def rule_1(ei_df, bl_df, bl_cols):
    # Rule 1.1
    # -- sender/receiver merge bl
    ei_df = pd.merge(ei_df, bl_df, how='left', left_on=['bra_id_s','cnae_id_s'], right_on=['bra_id', 'cnae_id'])
    ei_df.cnae_id_s[ei_df.d_bl == 1] = CNAE_BLACKLISTED
    ei_df.loc[ei_df.d_bl == 1, 'is_blacklisted'] = 1
    print "Blacklisting %s sending transactions" % (ei_df.cnae_id_s[ei_df.d_bl == 1].count())

    ei_df = ei_df.drop(labels=bl_cols, axis=1)
    ei_df = pd.merge(ei_df, bl_df, how='left', left_on=['bra_id_r','cnae_id_r'], right_on=['bra_id', 'cnae_id'])
    print "Blacklisting %s receiving transactions" % (ei_df.cnae_id_r[ei_df.d_bl == 1].count())
    ei_df.cnae_id_r[ei_df.d_bl == 1] = CNAE_BLACKLISTED
    ei_df.loc[ei_df.d_bl == 1, 'is_blacklisted'] = 1
    ei_df = ei_df.drop(labels=bl_cols, axis=1)

    # Rule 1.2
    bra_checker_df = bl_df.groupby(["bra_id"]).agg({"num_est": np.sum})
    too_small = bra_checker_df[bra_checker_df.num_est < 3]
    print too_small.num_est.count(), "BRAs have CNAEs on the BL with < 3 establishments"
    if too_small.num_est.count() > 0:
        raise Exception("NEED TO FILTER BRAs")

    # flatten_df_r = ei_df[["bra_id_r", "cnae_id_r", "is_blacklisted", "product_value"]]
    # flatten_df_r = flatten_df_r.rename(columns={"bra_id_r": "bra_id", "cnae_id_r": "cnae_id"})
    # flatten_df_s = ei_df[["bra_id_s", "cnae_id_s", "is_blacklisted", "product_value"]] 
    # flatten_df_s = flatten_df_s.rename(columns={"bra_id_s": "bra_id", "cnae_id_s": "cnae_id"})
    # flatten_df_s = flatten_df_s[flatten_df_s.is_blacklisted > 0]
    # flatten_df_r = flatten_df_r[flatten_df_r.is_blacklisted > 0]
    # flatten_df_raw = pd.concat([flatten_df_s, flatten_df_r])
    # print flatten_df_raw.head()
    # count_cnaes = flatten_df_raw.groupby(["bra_id"]).agg({"cnae_id":pd.Series.nunique})
    # print count_cnaes.head()

    return ei_df

def rule_2(ei_df):
    # Identify products that may need to be blacklisted, these are products 
    # any product transacted by CNAEs that are blacklisted with < two companies 

    # 2.1 identify product transacted by CNAE's on the BL
    # 2.2. If another CNAE transacts in the product it can be shown
    # 2.3. Even if two CNAE transact in a product and both are blacklisted. if more than two 
    #    CNAEs overall transact in the product, it can be show.
    # 2.4 If there is one CNAE selling a product and its blacklisted or two CNAEs selling a product 
    #     and both CNAEs blacklisted --> blacklist the product
    print "FILTER #2: HS FILTERING"

    flatten_df_r = ei_df[["bra_id_r", "cnae_id_r", "is_blacklisted", "hs_id", "product_value"]]
    flatten_df_r = flatten_df_r.rename(columns={"bra_id_r": "bra_id", "cnae_id_r": "cnae_id"})
    flatten_df_s = ei_df[["bra_id_s", "cnae_id_s", "is_blacklisted", "hs_id", "product_value"]] 
    flatten_df_s = flatten_df_s.rename(columns={"bra_id_s": "bra_id", "cnae_id_s": "cnae_id"})

    flatten_df_raw = pd.concat([flatten_df_s, flatten_df_r])
    
    # Assemble a dataframe that is bra, cnae and product (breakdown recevier/sender divide)
    flatten_df = flatten_df_raw.groupby(["bra_id", "cnae_id", "hs_id"]).agg({"is_blacklisted": np.sum})
    flatten_df = flatten_df.reset_index()
    flatten_df = flatten_df[(flatten_df.is_blacklisted > 0) & (flatten_df.bra_id.str.startswith("4mg"))]
    print flatten_df.head()

    print "Making counter DF..."
    # make a table with bra_id, cnae_id, hs_id Num companies
    counter_df = flatten_df_raw.groupby(["bra_id", "hs_id"]).agg({"cnae_id": pd.Series.nunique})
    counter_df.rename(columns={"cnae_id":"num_cnaes"}, inplace=True)
    counter_df = counter_df.reset_index()
    counter_df = counter_df[counter_df.bra_id.str.startswith("4mg")]
    checker_df = pd.merge(flatten_df, counter_df, how='left', left_on=['bra_id','hs_id'], right_on=['bra_id', 'hs_id'])
    to_blacklist = checker_df[(checker_df.is_blacklisted > 0) & (checker_df.num_cnaes < 3)]
    to_blacklist['hs_blacklist'] = 1
    to_blacklist = to_blacklist.drop(labels=["is_blacklisted"], axis=1)

    ei_df = pd.merge(ei_df, to_blacklist, how="left", left_on=["bra_id_s", "hs_id"], right_on=["bra_id", "hs_id"])
    ei_df.loc[ei_df.hs_blacklist == 1, 'hs_id'] = HS_BLACKLIST
    ei_df = ei_df.drop(labels=['is_blacklisted', "bra_id", "cnae_id", "num_cnaes", "hs_blacklist"], axis=1)
    ei_df = pd.merge(ei_df, to_blacklist, how="left", left_on=["bra_id_r", "hs_id"], right_on=["bra_id", "hs_id"])
    ei_df.loc[ei_df.hs_blacklist == 1, 'hs_id'] = HS_BLACKLIST
    print ei_df[ei_df.hs_id == HS_BLACKLIST].ncm.count(), " ROWS HAD PRODUCTS BLACKLISTED!"
 

    # 2.5 If fewer than 3 products are blacklisted in location, aggregate products with smallest transaction volumne
    #     in the location.
    bra_product_df = to_blacklist.groupby(["bra_id"]).agg({"hs_id": pd.Series.nunique})
    bra_product_df.rename(columns={"hs_id":"num_hs"}, inplace=True)
    bra_product_df = bra_product_df[bra_product_df.num_hs < 3]
    # -- These places need to have smallest transaction value aggregated in.

    print "For BRA, find next smallest HS. We will do two iterations."
    transaction_df = flatten_df_raw[flatten_df_raw.bra_id.str.startswith("4mg")].groupby(["bra_id", "hs_id"]).agg({"product_value": np.sum})
    transaction_df = transaction_df.reset_index()

    print "For each bra find the MIN product value sum."
    min_values_df = transaction_df.groupby(["bra_id"]).agg({"product_value":pd.Series.min})
    min_values_df = min_values_df.reset_index()
    min_values_df['is_min'] = 1
    
    print "Now recover which HS had the min product value"
    smallest_transaction_df = pd.merge(transaction_df, min_values_df, how="left", left_on=["bra_id", "product_value"], right_on=["bra_id", "product_value"])
    print smallest_transaction_df.head()

    second_smallest = smallest_transaction_df[smallest_transaction_df.is_min != 1]
    second_smallest = second_smallest.groupby(["bra_id"]).agg({"product_value":pd.Series.min})
    second_smallest['is_min2'] = 2
    second_smallest = second_smallest.reset_index()
    print "For each bra find second smallest product value sum."
    smallest_transaction_df = pd.merge(smallest_transaction_df, second_smallest, how="left", left_on=["bra_id", "product_value"], right_on=["bra_id", "product_value"])
    
    # print smallest_transaction_df[smallest_transaction_df.is_min2 == 2].head()
    smallest_transaction_df = smallest_transaction_df[(smallest_transaction_df.is_min == 1) | (smallest_transaction_df.is_min2 == 2)]
    print smallest_transaction_df.head()

    # -- next step FINISH application of last rule.
    bra_product_df = bra_product_df.reset_index()
    bra_product_df = pd.merge(bra_product_df, smallest_transaction_df[smallest_transaction_df.is_min == 1], how="left", left_on=["bra_id"], right_on=["bra_id"])
    bra_product_df = bra_product_df.drop(labels=["product_value", "is_min2"], axis=1)
    merge_second = smallest_transaction_df[smallest_transaction_df.is_min2 == 2]
    merge_second = merge_second.rename(columns={"hs_id": "hs_id2"})
    merge_second = merge_second[["bra_id", "hs_id2", "is_min2"]]

    print merge_second.head()
    bra_product_df = pd.merge(bra_product_df, merge_second, how="left", left_on=["bra_id"], right_on=["bra_id"])

    bra_product_df = bra_product_df.rename(columns={"hs_id": "hs_id1"})
    print bra_product_df.head()

    ei_df = ei_df.drop(labels=["bra_id", "cnae_id", "num_cnaes", "hs_blacklist"], axis=1)

    ei_df = hs_bl_in_city_to_three_products(ei_df, bra_product_df, bra_mode="bra_id_r")
    ei_df = hs_bl_in_city_to_three_products(ei_df, bra_product_df, bra_mode="bra_id_s")

    print ei_df[ei_df.hs_id == HS_BLACKLIST].hs_id.count()
    return ei_df

def rule_4(ei_df):
    print "APPLY rule 4"
    aggrules={"cnae_id": pd.Series.nunique}
    bp_bl_df = flatten_to_cnae(ei_df, ["bra_id", "hs_id"], aggrules, onlyblacklist=True)
    bp_bl_df = bp_bl_df.rename(columns={"cnae_id": "bl_cnaes"})
    bp_bl_df = bp_bl_df.reset_index()
    bl_all_df = flatten_to_cnae(ei_df, ["bra_id", "hs_id"], aggrules, onlyblacklist=False)
    bl_all_df = bl_all_df.rename(columns={"cnae_id": "total_cnaes"})
    bl_all_df = bl_all_df.reset_index()
    counter_df = pd.merge(bp_bl_df, bl_all_df, how='left', left_on=["bra_id", "hs_id"], right_on=["bra_id", "hs_id"])

    # print counter_df.bra_id.count()
    # print counter_df[counter_df.bl_cnaes == counter_df.total_cnaes].bra_id.count()
    # print counter_df.head()

    bp_only_bl = counter_df[counter_df.bl_cnaes == counter_df.total_cnaes]
    # START 4.1.i (less than 3 CNAEs) DO NOT SHOW PRODUCT (BL PRODUCT)
    print "Applying rule 4.i"
    hs_to_bl = bp_only_bl[bp_only_bl.bl_cnaes < 3]
    hs_to_bl = hs_to_bl.drop(labels=["total_cnaes"], axis=1)
    ei_df = pd.merge(ei_df, hs_to_bl, how='left', left_on=["bra_id_r", "hs_id"], right_on=["bra_id", "hs_id"])
    ei_df.loc[ei_df.bl_cnaes > 0, 'hs_id'] = HS_BLACKLIST
    round_1 =  ei_df[ei_df.bl_cnaes > 0].hs_id.count()
    ei_df =ei_df.drop(labels=["bra_id", "bl_cnaes"], axis=1)
    ei_df = pd.merge(ei_df, hs_to_bl, how='left', left_on=["bra_id_s", "hs_id"], right_on=["bra_id", "hs_id"])
    ei_df.loc[ei_df.bl_cnaes > 0, 'hs_id'] = HS_BLACKLIST
    print "4.i: Blacklisted # products", ei_df[ei_df.bl_cnaes > 0].hs_id.count() + round_1
    ei_df = ei_df.drop(labels=["bra_id", "bl_cnaes"], axis=1)
    # -- END of 4.1.i

    # START 4.1.ii
    cnae_to_bl = bp_only_bl[bp_only_bl.bl_cnaes >= 3]
    cnae_to_bl = cnae_to_bl.drop(labels=["total_cnaes"], axis=1)
    ei_df = pd.merge(ei_df, cnae_to_bl, how='left', left_on=["bra_id_r", "hs_id"], right_on=["bra_id", "hs_id"])
    ei_df.loc[ei_df.bl_cnaes > 0, 'cnae_id_r'] = CNAE_BLACKLISTED
    round_1 =  ei_df[ei_df.bl_cnaes > 0].hs_id.count()

    ei_df =ei_df.drop(labels=["bra_id", "bl_cnaes"], axis=1)
    ei_df = pd.merge(ei_df, cnae_to_bl, how='left', left_on=["bra_id_s", "hs_id"], right_on=["bra_id", "hs_id"])
    ei_df.loc[ei_df.bl_cnaes > 0, 'cnae_id_s'] = CNAE_BLACKLISTED
    
    print "4.ii: Blacklisted # products", ei_df[ei_df.bl_cnaes > 0].cnae_id_s.count() + round_1

    # print ei_df.head()
    # -- END of 4.ii
    
    # TODO 4.ii (gte 3 cnaes)
    # UNCERTAIN:::: CLASSIFY CNAES AS OTHERS????
    bp_mixed = counter_df[counter_df.bl_cnaes != counter_df.total_cnaes]
    # print bp_mixed.head(), "TEST"


    return ei_df

def blacklist(ei_df, blpath):
    # -- Do blacklist filtering
    bl_cols = ["bra_id", "cnae_id", "num_est", "d_bl"]
    bl_converters = {"bra_id" : lookup_location, "cnae_id": lookup_cnae}
    bl_df = pd.read_csv(blpath, header=0, sep=";", converters=bl_converters, names=bl_cols, quotechar="'", decimal=",")
    print bl_df.head()

    ei_df.is_blacklisted = 0

    print "APPLY RULE 1"
    ei_df = rule_1(ei_df, bl_df, bl_cols)
    # ei_df = rule_2(ei_df)
    # RULE 3 is implemented in Rule 1
    ei_df = rule_4(ei_df)


    return ei_df