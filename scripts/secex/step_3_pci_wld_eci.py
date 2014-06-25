# -*- coding: utf-8 -*-
"""

how to run this:
python -m scripts.secex.step_3_pci_wld_eci \
            -y 2000 \
            -e data/secex/observatory_ecis.csv \
            -p data/secex/observatory_pcis.csv \
            data/secex/export/2000

"""

''' Import statements '''
import csv, sys, os, bz2, time, click
import pandas as pd
from ..helpers import get_file, format_runtime

@click.command()
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.option('--delete', '-d', help='Delete the previous file?', is_flag=True, default=False)
@click.option('--eci', '-e', help='ECI file.', type=click.Path(exists=True), required=True)
@click.option('--pci', '-p', help='PCI file.', type=click.Path(exists=True), required=True)
@click.argument('data_dir', type=click.Path(exists=True), required=True)
def main(year, delete, eci, pci, data_dir):
    
    pci_file = get_file(pci)
    pcis = pd.read_csv(pci_file, sep=",", converters={"hs_id": str})
    pcis = pcis.set_index(["year", "hs_id"])
    
    eci_file = get_file(eci)
    ecis = pd.read_csv(eci_file, sep=",")
    ecis = ecis.set_index(["year", "wld_id"])

    yp_file_path = os.path.abspath(os.path.join(data_dir, 'yp.tsv.bz2'))
    yp_file = get_file(yp_file_path)
    yp = pd.read_csv(yp_file, sep="\t", converters={"hs_id": str})
    yp = yp.set_index(["year", "hs_id"])
    
    yw_file_path = os.path.abspath(os.path.join(data_dir, 'yw.tsv.bz2'))
    yw_file = get_file(yw_file_path)
    yw = pd.read_csv(yw_file, sep="\t", converters={"wld_id": str})
    yw = yw.set_index(["year", "wld_id"])
    
    yp["pci"] = pcis["pci"]
    yw["eci"] = ecis["eci"]
    
    '''
        write new files
    '''
    new_yp_file = os.path.abspath(os.path.join(data_dir, "yp_pcis.tsv.bz2"))
    print ' writing file:', new_yp_file
    yp.to_csv(bz2.BZ2File(new_yp_file, 'wb'), sep="\t", index=True)
    
    new_yw_file = os.path.abspath(os.path.join(data_dir, "yw_ecis.tsv.bz2"))
    print ' writing file:', new_yw_file
    yw.to_csv(bz2.BZ2File(new_yw_file, 'wb'), sep="\t", index=True)
    
    '''
        delete old files
    '''
    if delete:
        print "deleting previous files"
        os.remove(yp_file.name)
        os.remove(yw_file.name)
        

if __name__ == "__main__":
    start = time.time()
    
    main()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;