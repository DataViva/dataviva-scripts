import os
import click
import ntpath
import pandas as pd
import MySQLdb
import numpy as np
from pandas.tools.pivot import pivot_table

from table_aggregator import make_table


PURCHASES = [11, 22, 33, 44, 55, 66, 18, 29,40, 51, 62, 73]
TRANSFERS = [12, 23, 34, 45, 56, 67]
DEVOLUTIONS = [13, 24, 35, 46, 57, 68] 
SERVICES = [14, 25, 36, 47, 58, 69, 15, 26, 37, 48, 59, 70, 16, 27, 38, 49, 60, 71]
FIXED_ASSETS = [17, 28, 39, 50, 61, 72]
ICMS_CREDITS = [19, 30, 41, 52, 63, 74]
REMITS = [20, 31, 42, 53, 64, 75]
OTHERS = [21, 32, 43, 54, 65, 76]

# -- Load in metadata from DB
print "Getting municipal data from DB..."
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                     passwd=os.environ["DATAVIVA_DB_PW"], 
                     db=os.environ["DATAVIVA_DB_NAME"])
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

BRA_UNREPORTED = 'XX000007'
CNAE_NO_INFO = 'x00001'
#CNAE_DNA = 'x00002'

def lookup_location(x):
    if x == '-1':
        return BRA_UNREPORTED
    return lookup[x]

def update_hs_id(old_hs_id):
	return hs_lookup[str(old_hs_id)]

def lookup_cnae(x):
	if x == '1':
		return CNAE_NO_INFO
	if x == '2':
		return CNAE_NO_INFO
	return cnae_lookup[str(x)]

PURCHASE_ID = 1
TRANSFER_ID = 2
DEVOLUTION_ID = 3
SERVICE_ID = 4
FIXED_ASSET_ID = 5
ICMS_CREDIT_ID = 6
REMIT_ID = 7
OTHER_ID = 8

def look_cfop(x):
	x = int(x)

	if x in PURCHASES:
		return PURCHASE_ID
	elif x in TRANSFERS:
		return DEVOLUTION_ID
	elif x in DEVOLUTIONS:
		return DEVOLUTION_ID
	elif x in SERVICES:
		return SERVICE_ID
	elif x in FIXED_ASSETS:
		return FIXED_ASSET_ID
	elif x in ICMS_CREDITS:
		return ICMS_CREDIT_ID
	elif x in REMITS:
		return REMIT_ID
	elif x in OTHERS:
		return OTHER_ID
	raise Exception("Invalid or unknown transaction ID. What is (%s)?" % x)

@click.command()
@click.option('--fname', prompt='file name',
              help='Original file path to CSV.')
@click.option('--odir', default='.', prompt=False,
			  help='Directory for script output.')
def main(fname, odir):
	print "Reading data frame..."
	cols = ["ncm", "hs_id",
			"cnae_id_r", "cnae_id_s", "CFOP_ID",
			"CFOP_Reclassification", "CFOP_Flow", "Receiver_Type", "Sender_Type",
			"bra_id_r", "bra_id_s", "year", "month",
			"Receiver_Situation", "Sender_Situation", "transportation_cost", "ICMS_ST_Value",
			"ICMS_ST_RET_Value", "ICMS_Value", "IPI_Value", "PIS_Value", "COFINS_Value", "II_Value",
			"product_value", "ISSQN_Value", "Origin"]


	converters = {"hs_id": update_hs_id, "Municipality_ID_Sender":lookup_location, "Municipality_ID_Receiver":lookup_location, "EconomicAtivity_ID_CNAE_Receiver": lookup_cnae, 
				"EconomicAtivity_ID_CNAE_Sender":lookup_cnae, "CFOP_Reclassification":look_cfop} 

	ei_df = pd.read_csv(fname, header=0, sep=";", converters=converters, names=cols, quotechar="'", decimal=",")    

	# -- Filter out any rows that are ICMS Credits transactions or transfers
	# print "Filtering ICMS credits and transfers"
	print "Processing..."
	ei_df['icms_tax'] = ei_df.ICMS_ST_Value + ei_df.ICMS_Value 
	ei_df['tax'] = ei_df.icms_tax + ei_df.IPI_Value + ei_df.PIS_Value + ei_df.COFINS_Value + ei_df.II_Value + ei_df.ISSQN_Value

	ei_df['purchase_value'] = 0
	ei_df['transfer_value'] = 0
	ei_df['devolution_value'] = 0
	ei_df['service_value'] = 0
	ei_df['fixed_asset_value'] = 0
	ei_df['icms_credit_value'] = 0
	ei_df['remit_value'] = 0
	ei_df['other_value'] = 0


	ei_df['purchase_value'][ei_df["CFOP_Reclassification"] == PURCHASE_ID] = ei_df["product_value"]
	ei_df['transfer_value'][ei_df["CFOP_Reclassification"] == TRANSFER_ID] = ei_df["product_value"]
	ei_df['devolution_value'][ei_df["CFOP_Reclassification"] == DEVOLUTION_ID] = ei_df["product_value"]
	ei_df['service_value'][ei_df["CFOP_Reclassification"] == SERVICE_ID] = ei_df["product_value"]
	ei_df['fixed_asset_value'][ei_df["CFOP_Reclassification"] == FIXED_ASSET_ID] = ei_df["product_value"]
	ei_df['icms_credit_value'][ei_df["CFOP_Reclassification"] == ICMS_CREDIT_ID] = ei_df["product_value"]
	ei_df['remit_value'][ei_df["CFOP_Reclassification"] == REMIT_ID] = ei_df["product_value"]
	ei_df['other_value'][ei_df["CFOP_Reclassification"] == OTHER_ID] = ei_df["product_value"]

	print "Aggregating..."
	primary_key =  ['year', 'month', 'bra_id_s', 'cnae_id_s', 
					'bra_id_r', 'cnae_id_r',
					'hs_id']

	output_values = ["purchase_value", "transfer_value", "devolution_value", "service_value", "fixed_asset_value", "icms_credit_value", "remit_value", "other_value", "tax", "icms_tax", "transportation_cost"]

	output_name = ntpath.basename(fname).replace(".csv", "")

	print "Making tables..."
	ymsrp = make_table(ei_df, "ymsrp", output_values, odir, output_name)
	ymsr = make_table(ymsrp, "ymsr", output_values, odir, output_name)
	ymsp = make_table(ymsrp, "ymsp", output_values, odir, output_name)
	ymrp = make_table(ymsrp, "ymrp", output_values, odir, output_name)
	yms = make_table(ymsp, "yms", output_values, odir, output_name)
	ymr = make_table(ymrp, "ymr", output_values, odir, output_name)
	ymp = make_table(ymsp, "ymp", output_values, odir, output_name)
	
if __name__ == '__main__':
    main()
