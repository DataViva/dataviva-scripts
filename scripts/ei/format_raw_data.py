import os
import click
import ntpath
import pandas as pd
import MySQLdb
import numpy as np

from table_aggregator import make_table

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
CNAE_DNA = 'x00002'

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
		return CNAE_DNA
	return cnae_lookup[str(x)]

@click.command()
@click.option('--fname', prompt='file name',
              help='Original file path to CSV.')
@click.option('--odir', default='.', prompt=False,
			  help='Directory for script output.')
def main(fname, odir):
	print "Reading data frame..."
	cols = ["TransactedProduct_ID_NCM", "TransactedProduct_ID_HS",
			"EconomicAtivity_ID_CNAE_Receiver", "EconomicAtivity_ID_CNAE_Sender", "CFOP_ID",
			"CFOP_Reclassification", "CFOP_Flow", "Receiver_Type", "Sender_Type",
			"Municipality_ID_Receiver", "Municipality_ID_Sender", "Year", "Monthly",
			"Receiver_Situation", "Sender_Situation", "Cost_Value", "ICMS_ST_Value",
			"ICMS_ST_RET_Value", "ICMS_Value", "IPI_Value", "PIS_Value", "COFINS_Value", "II_Value",
			"Product_Value", "ISSQN_Value", "Origin"]
	delim = ";"
	converters = {"TransactedProduct_ID_HS": update_hs_id, "Municipality_ID_Sender":lookup_location, "Municipality_ID_Receiver":lookup_location} 
	for c in cols:
	    if "CNAE" in c:
	        converters[c] = lookup_cnae
	ei_df = pd.read_csv(fname, header=0, sep=delim, converters=converters, names=cols, quotechar="'", decimal=",")    
	
	print "Processing..."
	ei_df['icms_tax'] = ei_df.ICMS_ST_Value + ei_df.ICMS_ST_RET_Value + ei_df.ICMS_Value 
	ei_df['tax'] = ei_df.icms_tax + ei_df.IPI_Value + ei_df.PIS_Value + ei_df.COFINS_Value + ei_df.II_Value + ei_df.ISSQN_Value

	print "Aggregating..."
	primary_key =  ['Year', 'Monthly', 'Municipality_ID_Sender', 'EconomicAtivity_ID_CNAE_Sender', 
					'Municipality_ID_Receiver', 'EconomicAtivity_ID_CNAE_Receiver',
					'TransactedProduct_ID_HS'] # -- TODO: receive confirmation
	#ymbibip = ei_df.groupby(primary_key).aggregate(np.sum)
	# print "Saving to file..."
	output_values = ["Product_Value", "tax", "icms_tax", "Cost_Value"]
	output_name = ntpath.basename(fname).replace(".csv", "")
	#output_path = os.path.join(odir, "output_ymsrp_%s.csv" % output_name)
	# ymbibip.to_csv(output_path, delim, columns = output_values)

	print "Making smaller tables..."
	# make_small_tables(ymbibip, output_name)

	make_table(ei_df, "yms", output_values, odir, output_name)
	make_table(ei_df, "ymr", output_values, odir, output_name)
	make_table(ei_df, "ymp", output_values, odir, output_name)
	make_table(ei_df, "ymsr", output_values, odir, output_name)
	make_table(ei_df, "ymsp", output_values, odir, output_name)
	make_table(ei_df, "ymrp", output_values, odir, output_name)
	make_table(ei_df, "ymsrp", output_values, odir, output_name)


if __name__ == '__main__':
    main()
