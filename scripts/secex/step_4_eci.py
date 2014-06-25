# -*- coding: utf-8 -*-
"""

how to run this:
python -m scripts.secex.step_4_eci \
            -y 2000 \
            data/secex/export/2000

"""

''' Import statements '''
import csv, sys, os, bz2, time, click
import pandas as pd
from ..helpers import get_file, format_runtime
from ..growth_lib import growth

def get_ybp_rcas(data_dir, geo_level):
    
    ybp_file_path = os.path.abspath(os.path.join(data_dir, 'ybp.tsv.bz2'))
    ybp_file = get_file(ybp_file_path)
    ybp = pd.read_csv(ybp_file, sep="\t", converters={"hs_id":str})
    
    hs_criterion = ybp['hs_id'].map(lambda x: len(x) == 6)
    bra_criterion = ybp['bra_id'].map(lambda x: len(x) == geo_level)
    
    ybp = ybp[hs_criterion & bra_criterion]
    ybp = ybp.drop(["year"], axis=1)
    
    ybp = ybp.pivot(index="bra_id", columns="hs_id", values="val_usd").fillna(0)
    
    rcas = growth.rca(ybp)
    rcas[rcas >= 1] = 1
    rcas[rcas < 1] = 0
    
    return rcas

def get_yp_pcis(data_dir):
    
    yp_file_path = os.path.abspath(os.path.join(data_dir, 'yp_pcis.tsv.bz2'))
    yp_file = get_file(yp_file_path)
    yp = pd.read_csv(yp_file, sep="\t", converters={"hs_id":str})
    
    hs_criterion = yp['hs_id'].map(lambda x: len(x) == 6)
    
    yp = yp[hs_criterion & pd.notnull(yp['pci'])]
    yp = yp.drop(["year", "val_usd"], axis=1)
    yp = yp.set_index("hs_id")
    
    return yp.T

@click.command()
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.option('--delete', '-d', help='Delete the previous file?', is_flag=True, default=False)
@click.argument('data_dir', type=click.Path(exists=True), required=True)
def main(year, delete, data_dir):
    
    pcis = get_yp_pcis(data_dir)
    
    yb_file_path = os.path.abspath(os.path.join(data_dir, 'yb.tsv.bz2'))
    yb_file = get_file(yb_file_path)
    yb = pd.read_csv(yb_file, sep="\t", index_col=["bra_id"], converters={"year":str})
    
    ecis = []
    for geo_level in [2, 4, 7, 8]:
        print "geo_level",geo_level
        
        rcas = get_ybp_rcas(data_dir, geo_level)
        rcas = rcas.reindex(columns=pcis.columns)
        
        geo_level_pcis = pd.DataFrame([pcis.values[0]]*len(rcas.index), columns=pcis.columns, index=rcas.index)
        
        geo_level_ecis = rcas * geo_level_pcis
        geo_level_ecis = geo_level_ecis.sum(axis=1)
        geo_level_ecis = geo_level_ecis / rcas.sum(axis=1)
        
        ecis.append(geo_level_ecis)
    
    ecis = pd.concat(ecis)
    yb["eci"] = ecis

    # print out file
    new_yb_file = os.path.abspath(os.path.join(data_dir, "yb_ecis.tsv.bz2"))
    print ' writing file:', new_yb_file
    yb.to_csv(bz2.BZ2File(new_yb_file, 'wb'), sep="\t", index=True)
    
    '''
        delete old file
    '''
    if delete:
        print "deleting previous files"
        os.remove(yb_file.name)

if __name__ == "__main__":
    start = time.time()
    
    main()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;