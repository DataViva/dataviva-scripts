import os
import click
import ntpath
import pandas as pd
import MySQLdb
import numpy as np
from pandas.tools.pivot import pivot_table

from table_aggregator import make_table

PURCHASES = 1
TRANSFERS = 2
DEVOLUTIONS = 3
CREDITS = 4
REMITS = 5


# -- Load in metadata from DB
print "Getting municipal data from DB..."
db = MySQLdb.connect(host="127.0.0.1", user=os.environ["DATAVIVA2_DB_USER"], 
                     passwd=os.environ["DATAVIVA2_DB_PW"], 
                     db=os.environ["DATAVIVA2_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()
cursor.execute("select id_mdic, id_ibge, id from attrs_bra;")
lookup = {}
for mdic, ibge, bra_id in cursor.fetchall():
    lookup[str(mdic)] = bra_id
    lookup[str(ibge)] = bra_id

print "Getting Product code data from DB..."
cursor.execute("select substr(id, 3), id from attrs_hs where substr(id, 3) != '' and length(id) = 6;")
hs_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
hs_lookup["9991"] = "229999"
hs_lookup["9992"] = "229999"
hs_lookup["9998"] = "229999"
hs_lookup["9997"] = "229999"

cursor.execute("select substr(id,2,6), id from attrs_cnae;")
print "Getting CNAE data from DB..."
cnae_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
cursor.close()

BRA_UNREPORTED = '0XX000007'
CNAE_NO_INFO = 'x00'
CNAE_BLACKLISTED = 'x01'
CNAE_OTHER = 'x99'
HS_BLACKLIST = '230000'
#CNAE_DNA = 'x00002'

def lookup_location(x):
    if x == '-1':
        return BRA_UNREPORTED
    if x == '4128625':
    	x = '5200605'
    muni = lookup[x]
    if not muni.startswith("4mg"):
    	muni = muni[:3] # -- outside MG only use state level
    return muni

def update_hs_id(old_hs_id):
	return hs_lookup[str(old_hs_id)]

def lookup_cnae(x):
	if x in ['1', '-1']:
		return CNAE_NO_INFO
	if x in ['2', '-2']:
		return CNAE_NO_INFO
	return cnae_lookup[str(x)]


@click.command()
@click.option('--fname', prompt='file name',
              help='Original file path to CSV.')
@click.option('--blpath', prompt='blacklist file name',
              help='file path to blacklist CSV.')
@click.option('--odir', default='.', prompt=False,
			  help='Directory for script output.')
def main(fname, blpath, odir):
	print "Reading data frame HELLO..."

	cols = ["ncm", "hs_id",
			"EconomicAtivity_ID_CNAE_Receiver_5d",
			"cnae_id_r",
			"EconomicAtivity_ID_CNAE_Sender_5d",
			"cnae_id_s",
			"CFOP_ID",
			"Receiver_foreign",
			"Sender_foreign",
			"bra_id_r",
			"bra_id_s",
			"year",
			"month",
			"transportation_cost",
			"ICMS_ST_Value",
			"ICMS_Value",
			"IPI_Value",
			"PIS_Value",
			"COFINS_Value",
			"II_Value",
			"product_value",
			"ISSQN_Value"]

	converters = {"hs_id": update_hs_id, "bra_id_s":lookup_location, "bra_id_r":lookup_location, "cnae_id_r": lookup_cnae, 
				"cnae_id_s":lookup_cnae} 

	ei_df = pd.read_csv(fname, header=0, sep=";", converters=converters, names=cols, quotechar="'", decimal=",")    

	# -- Do blacklist filtering
	bl_cols = ["bra_id", "cnae_id", "num_est", "d_bl"]
	bl_converters = {"bra_id" : lookup_location, "cnae_id": lookup_cnae}
	bl_df = pd.read_csv(blpath, header=0, sep=";", converters=bl_converters, names=bl_cols, quotechar="'", decimal=",")

	# sender/receiver merge bl
	ei_df = pd.merge(ei_df, bl_df, how='left', left_on=['bra_id_s','cnae_id_s'], right_on=['bra_id', 'cnae_id'])
	ei_df.cnae_id_s[ei_df.d_bl == 1] = CNAE_BLACKLISTED
	print "Blacklisting %s sending transactions" % (ei_df.cnae_id_s[ei_df.d_bl == 1].count())
	
	ei_df = ei_df.drop(labels=bl_cols, axis=1)
	ei_df = pd.merge(ei_df, bl_df, how='left', left_on=['bra_id_r','cnae_id_r'], right_on=['bra_id', 'cnae_id'])
	print "Blacklisting %s receiving transactions" % (ei_df.cnae_id_r[ei_df.d_bl == 1].count())
	ei_df.cnae_id_r[ei_df.d_bl == 1] = CNAE_BLACKLISTED

	# -- HS blacklist
	ei_df.hs_id[(ei_df.cnae_id_r == CNAE_BLACKLISTED) & (ei_df.cnae_id_s == CNAE_BLACKLISTED)] = HS_BLACKLIST
	print "Blacklisting %s products" % (ei_df.hs_id[ei_df.hs_id == HS_BLACKLIST].count())

	# -- Filter out any rows that are ICMS Credits transactions or transfers
	# print "Filtering ICMS credits and transfers"
	print "Processing..."
	ei_df['icms_tax'] = ei_df.ICMS_ST_Value + ei_df.ICMS_Value 
	ei_df['tax'] = ei_df.icms_tax + ei_df.IPI_Value + ei_df.PIS_Value + ei_df.COFINS_Value + ei_df.II_Value + ei_df.ISSQN_Value

	ei_df['purchase_value'] = 0
	ei_df['transfer_value'] = 0
	ei_df['devolution_value'] = 0
	ei_df['icms_credit_value'] = 0
	ei_df['remit_value'] = 0

	ei_df['purchase_value'][ei_df["CFOP_ID"] == PURCHASES] = ei_df["product_value"]
	ei_df['transfer_value'][ei_df["CFOP_ID"] == TRANSFERS] = ei_df["product_value"]
	ei_df['devolution_value'][ei_df["CFOP_ID"] == DEVOLUTIONS] = ei_df["product_value"]
	ei_df['icms_credit_value'][ei_df["CFOP_ID"] == CREDITS] = ei_df["product_value"]
	ei_df['remit_value'][ei_df["CFOP_ID"] == REMITS] = ei_df["product_value"]

	print "Aggregating..."
	primary_key =  ['year', 'month', 'bra_id_s', 'cnae_id_s', 
					'bra_id_r', 'cnae_id_r',
					'hs_id']

	output_values = ["purchase_value", "transfer_value", "devolution_value", "icms_credit_value",  "remit_value", "tax", "icms_tax", "transportation_cost"]

	output_name = ntpath.basename(fname).replace(".csv", "")

	print "Making tables..."
	ymsrp = make_table(ei_df, "ymsrp", output_values, odir, output_name)
	ymsr = make_table(ei_df, "ymsr", output_values, odir, output_name)
	ymsp = make_table(ei_df, "ymsp", output_values, odir, output_name)
	ymrp = make_table(ei_df, "ymrp", output_values, odir, output_name)
	yms = make_table(ei_df, "yms", output_values, odir, output_name)
	ymr = make_table(ei_df, "ymr", output_values, odir, output_name)
	ymp = make_table(ei_df, "ymp", output_values, odir, output_name)
	
if __name__ == '__main__':
    main()
