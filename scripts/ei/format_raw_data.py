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
db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"], 
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

BRA_UNREPORTED = '0XX000007'
CNAE_NO_INFO = 'x00'
CNAE_BLACKLISTED = 'x01'
CNAE_OTHER = 'x99'
HS_BLACKLIST = '230000'


def lookup_location(x):
    if x == '-1':
        return BRA_UNREPORTED
    if x == '4128625':
    	x = '5200605'
    muni = lookup[x]
    # if not muni.startswith("4mg"):
    	# muni = muni[:3] # -- outside MG only use state level
    return muni

def update_hs_id(old_hs_id):
	return hs_lookup[str(old_hs_id)]

def lookup_cnae(x):
	if x in ['1', '-1']:
		return CNAE_NO_INFO
	if x in ['2', '-2']:
		return CNAE_NO_INFO
	return cnae_lookup[str(x)]

HDF_CACHE = "hdf_cache"

def _check_hdf_cache(input_file, output_path):
    print "CHECK HDF"
    print input_file
    print output_path
    file_name = os.path.basename(input_file)
    target = os.path.join(output_path, file_name + ".h5")
    if not os.path.exists(output_path):
        # -- make directory if it doesn't exist
        os.makedirs(output_path)

    if not os.path.exists(target):
        return (False, target)
    df = pd.read_hdf(target, HDF_CACHE)
    return (df, target)

@click.command()
@click.option('--fname', prompt='file name',
              help='Original file path to CSV.')
@click.option('--odir', default='.', prompt=False,
			  help='Directory for script output.')
def main(fname, odir):
	print "Reading data frame..."

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
	
	ei_df, target = _check_hdf_cache(fname, odir)

	if not ei_df:
		ei_df = pd.read_csv(fname, header=0, sep=";", converters=converters, names=cols, quotechar="'", decimal=",")    

		print "Processing..."
		ei_df['icms_tax'] = ei_df.ICMS_ST_Value + ei_df.ICMS_Value 
		ei_df['tax'] = ei_df.icms_tax + ei_df.IPI_Value + ei_df.PIS_Value + ei_df.COFINS_Value + ei_df.II_Value + ei_df.ISSQN_Value

		ei_df["purchase_value"] = 0
		ei_df["transfer_value"] = 0
		ei_df["devolution_value"] = 0
		ei_df["icms_credit_value"] = 0
		ei_df["remit_value"] = 0

		ei_df.loc[ei_df.CFOP_ID == PURCHASES, "purchase_value"] = ei_df.product_value
		ei_df.loc[ei_df.CFOP_ID == TRANSFERS, "transfer_value"] = ei_df.product_value
		ei_df.loc[ei_df.CFOP_ID == DEVOLUTIONS, "devolution_value"] = ei_df.product_value
		ei_df.loc[ei_df.CFOP_ID == CREDITS, "icms_credit_value"] = ei_df.product_value
		ei_df.loc[ei_df.CFOP_ID == REMITS, "remit_value"] = ei_df.product_value
		
		ei_df.to_hdf(target, HDF_CACHE, append=False)


	print "Aggregating..."
	primary_key =  ['year', 'month', 'bra_id_s', 'cnae_id_s', 
					'bra_id_r', 'cnae_id_r',
					'hs_id']

	output_values = ["purchase_value", "transfer_value", "devolution_value", "icms_credit_value",  "remit_value", "tax", "icms_tax", "transportation_cost"]

	output_name = ntpath.basename(fname).replace(".csv", "")

	print "Making tables..."
	ymsr = make_table(ei_df, "ymsr", output_values, odir, output_name)
	yms = make_table(ei_df, "yms", output_values, odir, output_name)
	ymr = make_table(ei_df, "ymr", output_values, odir, output_name)
	
if __name__ == '__main__':
    main()
