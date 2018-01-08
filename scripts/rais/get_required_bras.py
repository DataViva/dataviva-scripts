# -*- coding: utf-8 -*-
''' Import statements '''
import os, sys, time, bz2, click, fnmatch, MySQLdb, pickle
import pandas as pd

from _required import required

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
    
    d = pd.HDFStore(os.path.join(output_path, 'rais_df_raw.h5'))
    if "yb" in d:
        tables = {"yb":d["yb"], "yo":d["yo"], "yi":d["yi"], "ybi":d["ybi"], "ybo":d["ybo"], "yio":d["yio"], "ybio":d["ybio"]}
    else:
        sys.exit('need rais_df_raw.h5 file')
    
    req_bras = required(tables["ybio"], tables["ybi"], tables["yi"], year, depths, True)
    
    # for bra in req_bras:
        # print len(req_bras[bra])
        # print req_bras[bra][:3]
        # cursor.executemany("update rais_ybi set required_bras = %s where year=%s and bra_id=%s and cnae_id=%s;", req_bras[bra])
        # for cnae in req_bras[bra]:
        #     reqs = '[' + ', '.join(['"{}"'.format(r) for r in req_bras[bra][cnae]]) + ']'
        #     print [reqs, year, bra, cnae]
        #     # sys.exit()
        #     cursor.execute("update rais_ybi set required_bras = %s where year=%s and bra_id=%s and cnae_id=%s;", [reqs, year, bra, cnae])

    # d["req_bras"] = req_bras
    # with open(os.path.join(output_path, 'req_bras.pickle'), 'wb') as f:
    #     pickle.dump(dict(req_bras), f)

    print("--- %s minutes ---" % str((time.time() - start)/60))

if __name__ == "__main__":
    main()
