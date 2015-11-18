# -*- coding: utf-8 -*-
''' Import statements '''
import os, sys, time, bz2, click, fnmatch, MySQLdb, pickle
import pandas as pd

from _rdo_temp import rdo

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), 
                     user=os.environ["DATAVIVA_DB_USER"], 
                     passwd=os.environ["DATAVIVA_DB_PW"], 
                     db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

@click.command()
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main( year, output_path):
    print; print "~~~**** YEAR: {0} ****~~~".format(year); print;
    start = time.time()
    step = 0
    # regions state, meso, micro, planning region, munic
    depths = {
        "bra": [1, 3, 5, 7, 8, 9],
        "cnae": [1, 3, 6],
        "cbo": [1, 4],
        "demo": [1, 4]
    }
    
    d = pd.HDFStore(os.path.join(output_path, 'secex.h5'))
    if "ymb" in d:
        tables = {}
        tables["ymb"] = d["ymb"]; tables["ymp"] = d["ymp"]; tables["ymw"] = d["ymw"]; tables["ymbp"] = d["ymbp"]; tables["ymbw"] = d["ymbw"]; tables["ympw"] = d["ympw"]; tables["ymbpw"] = d["ymbpw"]
        # print tables["ymp"].head()
        
        rdo(tables["ymbp"], tables["ymp"], year, depths["bra"])
    
    print("--- %s minutes ---" % str((time.time() - start)/60))

if __name__ == "__main__":
    main()
