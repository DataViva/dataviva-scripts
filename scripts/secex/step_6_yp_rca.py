# -*- coding: utf-8 -*-
"""

run this: 
python -m scripts.secex.step_6_yp_rca \
            -y 2000 \
            data/secex/export/2000

"""

''' Import statements '''
import csv, sys, os, MySQLdb, time, bz2, click
from collections import defaultdict
import pandas as pd
import pandas.io.sql as sql
from ..helpers import get_file, format_runtime

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def get_brazil_rcas(year):
    
    '''Get world values from database'''
    q = "select year, hs_id, rca from comtrade_ypw where year = {0} and "\
        "wld_id = 'sabra'".format(year)
    bra_rcas = sql.read_frame(q, db, index_col=["year", "hs_id"])
    return bra_rcas

@click.command()
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.option('--delete', '-d', help='Delete the previous file?', is_flag=True, default=False)
@click.argument('data_dir', type=click.Path(exists=True), required=True)
def main(year, delete, data_dir):
    print year
    
    print "loading yp..."
    yp_file_path = os.path.abspath(os.path.join(data_dir, 'yp_pcis_diversity.tsv.bz2'))
    yp_file = get_file(yp_file_path)
    yp = pd.read_csv(yp_file, sep="\t", converters={"hs_id": str})
    yp = yp.set_index(["year", "hs_id"])
    
    brazil_rcas = get_brazil_rcas(year)
    
    yp["rca"] = brazil_rcas["rca"]
    
    # print out files
    new_yp_file_path = os.path.abspath(os.path.join(data_dir, 'yp_pcis_diversity_rcas.tsv.bz2'))
    print ' writing file:', new_yp_file_path
    yp.to_csv(bz2.BZ2File(new_yp_file_path, 'wb'), sep="\t", index=True)
    
    if delete:
        print "deleting previous file"
        os.remove(yp_file.name)

if __name__ == "__main__":
    start = time.time()
    
    main()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;