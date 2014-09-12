import os
import click
import ntpath
import pandas as pd
import MySQLdb
import numpy as np
from pandas.tools.pivot import pivot_table

from table_aggregator import make_table

REMITS = [20, 31, 53, 64]
DEVOLUTIONS = [13, 24, 35, 46, 57, 68] 
DEVOLUTION_OR_REMIT = DEVOLUTIONS + REMITS
ICMS_CREDIT_OR_TRANSFER = [12, 19, 23, 30, 45, 52, 56, 63]
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
				"EconomicAtivity_ID_CNAE_Sender":lookup_cnae} 

	ei_df = pd.read_csv(fname, header=0, sep=";", converters=converters, names=cols, quotechar="'", decimal=",")    

	# -- Filter out any rows that are ICMS Credits transactions or transfers
	print "Filtering ICMS credits and transfers"
	ei_df = ei_df[~ei_df.CFOP_Reclassification.isin(ICMS_CREDIT_OR_TRANSFER)] 

	print "Processing..."
	ei_df['icms_tax'] = ei_df.ICMS_ST_Value + ei_df.ICMS_Value 
	ei_df['tax'] = ei_df.icms_tax + ei_df.IPI_Value + ei_df.PIS_Value + ei_df.COFINS_Value + ei_df.II_Value + ei_df.ISSQN_Value

	ei_df["remit_value"] = 0
	ei_df["devolved_value"] = 0

	ei_df["remit_value"][ei_df["CFOP_Reclassification"].isin(REMITS)] = ei_df["product_value"]
	ei_df["devolved_value"][ei_df["CFOP_Reclassification"].isin(DEVOLUTIONS)] = ei_df["product_value"]

	# print "Adjusting values..."
	ei_df["product_value"][ei_df["CFOP_Reclassification"].isin(DEVOLUTION_OR_REMIT)] = -ei_df["product_value"]


	#ei_df['value_returned'] = ei_df.apply(lambda x: x["product_value"] if x["CFOP_Reclassification"] in REMMITANCES else 0, axis=1)
	#ei_df['value_devolved'] = ei_df.apply(lambda x: x["product_value"] if x["CFOP_Reclassification"] in DEVOLUTIONS else 0, axis=1)

	print "Aggregating..."
	primary_key =  ['year', 'month', 'bra_id_s', 'cnae_id_s', 
					'bra_id_r', 'cnae_id_r',
					'hs_id'] 
	#ymbibip = ei_df.groupby(primary_key).aggregate(np.sum)
	# print "Saving to file..."
	output_values = ["product_value", "tax", "icms_tax", "transportation_cost", "remit_value", "devolved_value"]
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
