import os
import click
import ntpath
import pandas as pd
import MySQLdb

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
cursor.close()

def lookup_location(x):
    if x == '-1':
        return x
    return lookup[x]

@click.command()
@click.option('--fname', prompt='file name',
              help='Original file path to CSV.')
@click.option('--odir', default='.', prompt=False,
			  help='Directory for script output.')
def main(fname, odir):
	print "Reading data frame..."
	cols = ["TransactedProduct_ID_NCM", "TransactedProduct_ID_HS",
			"EconomicAtivity_ID_CNAE_Receiver", "EconomicAtivity_ID_CNAE_Sender",
			"CFOP_Reclassification", "CFOP_Flow", "Receiver_Type", "Sender_Type",
			"Municipality_ID_Receiver", "Municipality_ID_Sender", "Year", "Monthly",
			"Receiver_Situation", "Sender_Situation", "Cost_Value", "ICMS_ST_Value",
			"ICMS_ST_RET_Value", "ICMS_Value", "IPI_Value", "PIS_Value", "COFINS_Value", "II_Value",
			"Product_Value", "ISSQN_Value", "Origin"]
	delim = ";"
	converters = {"TransactedProduct_ID_HS": str, "Municipality_ID_Sender":lookup_location, "Municipality_ID_Receiver":lookup_location} 
	for c in cols:
	    if "CNAE" in c:
	        converters[c] = str
	ei_df = pd.read_csv(fname, header=0, sep=delim, converters=converters, names=cols, quotechar="'", decimal=",")    


	print "Aggregating..."
	primary_key =  ['Year', 'Monthly', 'Municipality_ID_Sender', 'EconomicAtivity_ID_CNAE_Sender', 
					'Municipality_ID_Receiver', 'EconomicAtivity_ID_CNAE_Receiver',
					'TransactedProduct_ID_HS', 'Origin']
	aggs = ei_df.groupby(primary_key).aggregate(sum)
	aggs['icms_tax'] = aggs.ICMS_ST_Value + aggs.ICMS_ST_RET_Value + aggs.ICMS_Value 
	aggs['tax'] = aggs.icms_tax + aggs.IPI_Value + aggs.PIS_Value + aggs.COFINS_Value + aggs.II_Value + aggs.ISSQN_Value


	print "Saving to file..."
	output_values = ["Product_Value", "tax", "icms_tax", "Cost_Value"]

	output_name = ntpath.basename(fname).replace(".csv", "")
	output_path = os.path.join(odir, "output_%s.csv" % output_name)
	aggs.to_csv(output_path, delim, columns = output_values)

if __name__ == '__main__':
    main()
