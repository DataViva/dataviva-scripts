import pandas as pd
import numpy as np
import collections
import click
from table_aggregator import make_table

from ei_config import cols, converters, lookup_location, lookup_cnae
from ei_config import CNAE_BLACKLISTED, HS_BLACKLISTED
from ei_config import PURCHASES, DEVOLUTIONS, REMITS, CREDITS, TRANSFERS
RECEIVER, SENDER = "receiver", "sender"


'''
EXAMPLE USAGE:

python format_raw_new.py --fname path/to/ei_2013_01.csv --blpath path/to/BlackList_2013_01.csv --odir output/path -y 2013 -m 1
'''

def mylen(x):
    return len(x) if isinstance(x, collections.Iterable) else 1

def setup(df, blacklist_df):
    ''' Calculate value columns from the raw data that will eventually be stored in the database
        Perform intial grouping of data into the deepest nesting form (year, month, sender, receiver, product)
        while summing values aggregating the unique CNAEs'''

    df['icms_tax'] = df.ICMS_ST_Value + df.ICMS_Value 
    df['tax'] = df.icms_tax + df.IPI_Value + df.PIS_Value + df.COFINS_Value + df.II_Value + df.ISSQN_Value

    df["purchase_value"] = df.apply(lambda x: x["product_value"] if x["CFOP_ID"] == PURCHASES else 0, axis=1)
    df["transfer_value"] = df.apply(lambda x: x["product_value"] if x["CFOP_ID"] == TRANSFERS else 0, axis=1)
    df["devolution_value"] = df.apply(lambda x: x["product_value"] if x["CFOP_ID"] == DEVOLUTIONS else 0, axis=1)
    df["icms_credit_value"] = df.apply(lambda x: x["product_value"] if x["CFOP_ID"] == CREDITS else 0, axis=1)
    df["remit_value"] = df.apply(lambda x: x["product_value"] if x["CFOP_ID"] == REMITS else 0, axis=1)

    def uniq_ests(bra, cnae):
        ''' Given a BRA and a CNAE or a BRA and a list of CNAEs compute the total number of establishments'''
        if not isinstance(cnae, collections.Iterable):
            cnae = [cnae]
        total = 0

        if bra.startswith("0XX"):
            return 10000
        elif not bra.startswith("4mg"):
            return 3 * len(cnae)

        for x in cnae:
            if x == "x00":
                total +=3; 
            else:
                try:
                    total += blacklist_df.xs([bra, x]).num_est
                except KeyError:
                    ''' Per Elton, we treat items not in the blacklist as if they have at least 3 establishments '''
                    total += 3
                    
        return total
    
    print "Doing groupby..."
    ''' Groups the raw data by bra_id_r, cnae_id_r, bra_id_s, cnae_id_s, and hs '''    
    grouped_df = df.groupby(["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"]).agg({
                "cnae_r_og": lambda x: set.union(set(x)),
                "cnae_s_og" : lambda x: set.union(set(x)),

                "purchase_value" : pd.Series.sum,
                "transfer_value" : pd.Series.sum,
                "devolution_value" : pd.Series.sum,
                "icms_credit_value" : pd.Series.sum,
                "icms_tax" : pd.Series.sum,
                "tax" : pd.Series.sum,
    })

    grouped_df = grouped_df.reset_index()
    print "Doing uniq ests..."
    ''' Calculates: the number of establishments (for sender/receiver BRA/CNAE pairs) and the number of unique CNAEs
        for senders and receivers'''
    grouped_df['num_est_r'] = grouped_df.apply(lambda x: uniq_ests(x["bra_id_r"], x["cnae_r_og"]), axis=1)
    grouped_df['num_est_s'] = grouped_df.apply(lambda x: uniq_ests(x["bra_id_s"], x["cnae_s_og"]), axis=1)
    grouped_df["num_cnae_r"] = grouped_df.cnae_r_og.map(mylen)
    grouped_df["num_cnae_s"] = grouped_df.cnae_s_og.map(mylen)
    print "Done with setup"
    return grouped_df, blacklist_df

###################################################################################
# RULE 1
# Blacklist the CNAE ID of any row where the sender or receiver CNAE is blacklisted 
# and the number of the corresponding establishments (sender/receiver) is less
# than 3. 
###################################################################################
def rule1(grouped_df, blacklist_df, mode):
    ''' Accepts a dataframe of the data in YMSRP form, a blacklist dataframe and a mode (either SENDER or RECEIVER) '''

    ''' Depenindg on the mode, look for rows where the sender or receiver is in violation of Rule 1.
        A rule 1 violation occurs when a CNAE is blacklisted but the corresponding number of establishments is less than 3.'''
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

    ''' Construct a dataframe of all the rows that violate rule '''
    bad_df = grouped_df[ bad_df_cond ] 

    ''' Count the number of violations '''
    violations_count = bad_df.bra_id_r.count()
    print "THERE ARE %s rule 1 violations with MODE=%s" % (violations_count, mode)

    ''' If there are no violations of the rule, there is nothing further to be done '''
    if not violations_count:
        print "FOUND no violations, no updates needed"
        return grouped_df

    ''' If there are violations of the rules, we setup a dataframe of good rows which can be added to the bad rows, so that they
        no longer violate rule #1. We look for rows where the bra_id_s or bra_id_r matches a row that is in the violation list
        and where the CNAE is not blacklisted. '''
    if mode == RECEIVER:
        good_df_cond = (grouped_df.bra_id_r.isin(bad_df.bra_id_r)) & (grouped_df.cnae_id_r != CNAE_BLACKLISTED)
    else:
        good_df_cond = (grouped_df.bra_id_s.isin(bad_df.bra_id_s)) & (grouped_df.cnae_id_s != CNAE_BLACKLISTED)

    # good df stores the minimum values
    good_df = grouped_df[ good_df_cond ]
    ''' Calculate the row with the minimum purchase_value to choose which row will be used to pad the values in violation of rule 1.
        Rows are aggregated by bra_id_x, bra_id_y, cnae_id_y, hs_id, where x & y can be sender or receiver respectively.  '''
    good_df = grouped_df.iloc[ good_df.groupby(agg_pk).agg({"purchase_value": pd.Series.idxmin}).purchase_value ]

    ''' Merge bad_df with good_df to fix bad_df based on values from good_dfs '''
    bad_df = pd.merge(bad_df, good_df, how='left', left_on=agg_pk, right_on=agg_pk, suffixes=["_bad", "_good"])
    ''' Only look at rows which have corresponding good values '''
    bad_df = bad_df[bad_df.purchase_value_good.notnull()]

    ''' Update the bad_df to fix the rule 1 violation. Combine the bad and good columns to update all the values. '''
    bad_df['purchase_value'] = bad_df.purchase_value_bad + bad_df.purchase_value_good
    bad_df['transfer_value'] = bad_df.transfer_value_bad + bad_df.transfer_value_good
    bad_df['devolution_value'] = bad_df.devolution_value_bad + bad_df.devolution_value_good
    bad_df['icms_credit_value'] = bad_df.icms_credit_value_bad + bad_df.icms_credit_value_good
    bad_df['icms_tax'] = bad_df.icms_tax_bad + bad_df.icms_tax_good
    bad_df['tax'] = bad_df.tax_bad + bad_df.tax_good

    ''' Depending on whether we are doing a sender or receiver update, add the number of establishments and increment the number of CNAEs by 1 '''
    if mode == RECEIVER:
        bad_df['num_est_r'] = bad_df.num_est_r_bad + bad_df.num_est_r_good
        bad_df['num_cnae_r'] = bad_df.num_cnae_r_bad + 1
        bad_df['num_cnae_s'] = bad_df.num_cnae_s_bad # -- Don't add here because this is receiver mode
        bad_df['num_est_s'] = bad_df.num_est_s_bad # -- Don't add here
    else:
        bad_df['num_est_s'] = bad_df.num_est_s_bad + bad_df.num_est_s_good
        bad_df['num_cnae_s'] = bad_df.num_cnae_s_bad + 1
        bad_df['num_cnae_r'] = bad_df.num_cnae_r_bad 
        bad_df['num_est_r'] = bad_df.num_est_r_bad

    ''' By definition, every CNAE in the bad_df has its CNAE Blacklisted, this is just confirmed after the merge process '''
    bad_df[cnae_lbl] = CNAE_BLACKLISTED

    ''' Track of the unique CNAEs that are in the new bad_df as a result of the updates with the good_df '''
    bad_df['cnae_r_og'] = bad_df.apply(lambda x: x["cnae_r_og_bad"].union(x["cnae_r_og_good"]), axis=1)
    bad_df['cnae_s_og'] = bad_df.apply(lambda x: x["cnae_s_og_bad"].union(x["cnae_s_og_good"]), axis=1)

    ''' Now that we've updated the bad_df with some values from the good_df we need to go back to the master_df and remove the rows
        we used to fix bad_df from the master table, so we don't double count anything. '''
    to_remove = bad_df[["bra_id_r", cnae_lbl + "_good", "bra_id_s", cnae_other, "hs_id"]]
    to_remove = to_remove.rename(columns={cnae_lbl+"_good": cnae_lbl})
    to_remove["delete"] = 1
    grouped_df = pd.merge(grouped_df, to_remove, how="left", left_on=["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"], right_on=["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"])
    grouped_df = grouped_df.loc[grouped_df.delete != 1]
    del grouped_df["delete"]


    ''' After we delete the rows used to fix rule 1 violations, we now update the master dataframe with the updated values from bad_df '''
    bad_df = bad_df[["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id", "purchase_value", "transfer_value", "devolution_value", "icms_credit_value", "icms_tax", "tax",  "num_est_r", "num_cnae_r", "num_est_s", "num_cnae_s", "cnae_r_og", "cnae_s_og"]]
    ''' Marker for which rows need updates '''
    bad_df["update_row"] = 1
    ''' Merge in preperation for dataframe update '''
    grouped_df = pd.merge(grouped_df, bad_df, how="left", left_on=["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"], right_on=["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"], suffixes=["_orig", "_fixed"])
    ''' For each row either update its value with the fixed value or keep the original value '''
    grouped_df.loc[grouped_df.update_row != 1, 'purchase_value'] = grouped_df.purchase_value_orig
    grouped_df.loc[grouped_df.update_row == 1, 'purchase_value'] = grouped_df.purchase_value_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'transfer_value'] = grouped_df.purchase_value_orig
    grouped_df.loc[grouped_df.update_row == 1, 'transfer_value'] = grouped_df.purchase_value_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'devolution_value'] = grouped_df.devolution_value_orig
    grouped_df.loc[grouped_df.update_row == 1, 'devolution_value'] = grouped_df.devolution_value_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'icms_credit_value'] = grouped_df.icms_credit_value_orig
    grouped_df.loc[grouped_df.update_row == 1, 'icms_credit_value'] = grouped_df.icms_credit_value_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'icms_tax'] = grouped_df.icms_tax_orig
    grouped_df.loc[grouped_df.update_row == 1, 'icms_tax'] = grouped_df.icms_tax_fixed
    grouped_df.loc[grouped_df.update_row != 1, 'tax'] = grouped_df.tax_orig
    grouped_df.loc[grouped_df.update_row == 1, 'tax'] = grouped_df.tax_fixed
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

    ''' Keep only the columns we need and discard the rest '''
    grouped_df = grouped_df[["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id", "cnae_s_og", "cnae_r_og", "purchase_value", "transfer_value", "devolution_value", "icms_credit_value", "icms_tax", "tax", "num_cnae_r", "num_est_r", "num_est_s","num_cnae_s"]]

    return grouped_df


#################################################################################
# RULE 2                  
# Blacklist the HS ID of any row where the sender or receiver CNAE is blacklisted 
# and the total number of CNAEs (sender+receiver) is less than 3.
#################################################################################
def rule2(grouped_df):
    ''' Blacklist any row where the sender or receiver CNAE is blacklisted and the total number of CNAEs is less than 3 '''

    ''' Compute total number of CNAEs '''
    grouped_df['total_cnaes'] = grouped_df.apply(lambda x: len( set(x["cnae_r_og"].union(x["cnae_s_og"])) ), axis=1)
    ''' Track original product for this row, before it is blacklisted '''
    grouped_df['hs_id_og'] = grouped_df['hs_id']
    ''' Blacklist products in violation of rule 2 (anywhere where there is a blacklisted CNAE and less than 3 products) '''
    grouped_df.loc[ ( (grouped_df.cnae_id_r == CNAE_BLACKLISTED) | (grouped_df.cnae_id_s == CNAE_BLACKLISTED) ) & (grouped_df.total_cnaes < 3) , 'hs_id' ] = HS_BLACKLISTED

    ''' Collapse the rows to aggregate the blacklisted products '''
    grouped_df = grouped_df.groupby(["bra_id_r", "cnae_id_r", "bra_id_s", "cnae_id_s", "hs_id"]).agg({
            "cnae_s_og" : lambda x: set.union(*list(x)),
            "cnae_r_og" : lambda x: set.union(*list(x)),
            "purchase_value": pd.Series.sum,
            "transfer_value": pd.Series.sum,
            "devolution_value": pd.Series.sum,
            "icms_credit_value": pd.Series.sum,
            "icms_tax": pd.Series.sum,
            "tax" : pd.Series.sum, 
            "num_cnae_r": pd.Series.sum,
            "num_cnae_s" : pd.Series.sum,
            "hs_id_og": lambda x: set.union(set(x)),
    }).reset_index()

    ''' Compute the number of HS ids per row '''
    grouped_df["hs_set_len"] = grouped_df.hs_id_og.apply(lambda x: len(x))

    bad_df = grouped_df[(grouped_df.hs_id == HS_BLACKLISTED) & ( grouped_df.hs_set_len < 3)  ]
    bad_count = bad_df.bra_id_r.count()
    if bad_count > 0:
        ''' At this point the products have been blacklisted and merged with rows, check if even after merging as possible
            there are any hs products which still have fewer than 3 products.
        '''
        print "NOTIFICATION [RULE 2]: there are %s products on the blacklist with fewer than 3 products and cannot be collapsed further" % (bad_count)
        # print bad_df.head()

    return grouped_df

def bl_prepare(ei_df, blpath):
    ''' This function prepares the blacklist dataframe and update the original dataframe so we can track the number of CNAEs
        after the CNAE ID is changed to blacklisted'''

    ''' Store the original CNAE information so we can track it for blacklisting purposes after the CNAE_BLACKLISTED
        is changed to blacklisted.'''
    ei_df["cnae_r_og"] = ei_df.cnae_id_r
    ei_df["cnae_s_og"] = ei_df.cnae_id_s

    bl_cols = ["bra_id", "cnae_id", "num_est", "d_bl"]
    bl_converters = {"bra_id" : lookup_location, "cnae_id": lookup_cnae}
    bl_df = pd.read_csv(blpath, header=0, sep=";", converters=bl_converters, names=bl_cols, quotechar="'", decimal=",")

    ''' Blacklist senders  '''
    ei_df = pd.merge(ei_df, bl_df, how='left', left_on=['bra_id_s','cnae_id_s'], right_on=['bra_id', 'cnae_id'])
    ei_df.loc[ei_df.d_bl == 1, 'cnae_id_s'] = CNAE_BLACKLISTED
    print "Blacklisting %s sending transactions" % (ei_df.cnae_id_s[ei_df.d_bl == 1].count())
    
    ''' Blacklist receivers '''
    ei_df = ei_df.drop(labels=bl_cols, axis=1)
    ei_df = pd.merge(ei_df, bl_df, how='left', left_on=['bra_id_r','cnae_id_r'], right_on=['bra_id', 'cnae_id'])
    print "Blacklisting %s receiving transactions" % (ei_df.cnae_id_r[ei_df.d_bl == 1].count())
    ei_df.loc[ei_df.d_bl == 1, 'cnae_id_r'] = CNAE_BLACKLISTED
    

    bl_df = bl_df.set_index(["bra_id", "cnae_id"])
    
    return ei_df, bl_df

@click.command()
@click.option('--fname', prompt='file name',
              help='Original file path to CSV.')
@click.option('--blpath', prompt='blacklist file name',
              help='file path to blacklist CSV.')
@click.option('--odir', default='.', prompt=False,
              help='Directory for script output.')
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('-m', '--month', prompt='Month', help='month of the data to convert', required=True)
def main(fname, blpath, odir, year, month):
    print "Applying EI Rules 1 and 2."
    ei_df = pd.read_csv(fname, header=0, sep=";", converters=converters, names=cols, quotechar="'", decimal=",")
    ei_df, bl_df = bl_prepare(ei_df, blpath)
    print "Doing setup..."
    ei_df, bl_df = setup(ei_df, bl_df)
    print "Entering rule 1..."
    ei_df = rule1(ei_df, bl_df, RECEIVER)
    ei_df = rule1(ei_df, bl_df, SENDER)
    print "Entering rule 2..."
    ei_df = rule2(ei_df)
    print ei_df
    output_values = ["purchase_value", "transfer_value", "devolution_value", "icms_credit_value",  "remit_value", "tax", "icms_tax", "transportation_cost", "year", "month"]
    output_name = "%s_%s" % (year,month)
    print "Making tables..."
    ymsrp = make_table(ei_df, "srp", output_values, odir, output_name, year=year, month=month)

if __name__ == "__main__":
    main()