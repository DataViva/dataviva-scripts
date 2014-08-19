# _aggregate.py
import numpy as np
import os

def make_small_table(ymbibip, table_name, output_values, odir, output_name):
	lookup = {
		"y" : ["Year"],
		"m" : ["Monthly"],
		"s" : ['Municipality_ID_Sender', 'EconomicAtivity_ID_CNAE_Sender'],
		"r" : ['Municipality_ID_Receiver', 'EconomicAtivity_ID_CNAE_Receiver'],
		"p" : ["TransactedProduct_ID_HS"]

	}
	pk_cols = []
	for letter in table_name:
		pk_cols += lookup[letter]
	print "PK_cols" , pk_cols
	table = ymbibip.groupby(pk_cols).aggregate(np.sum)
	output_path = os.path.join(odir, "output_%s_%s.csv" % (table_name, output_name))
	table.to_csv(output_path, ";", columns = output_values)
