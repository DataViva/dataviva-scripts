# -*- coding: utf-8 -*-
"""
    Format SECEX data for DB entry
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Columns:
    0:  Year
    1:  Month
    2:  DestinationCoutnry_ID
    3:  State_ID
    4:  Customs_Unit_Boarding_ID
    5:  Municipality_ID
    6:  Unit_ID
    7:  Quantity
    8:  TransactionAmount_kg
    9:  TransactionAmount_US$_FOB
    10: TransactedProduct_ID_HS
    
    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python \
    -m scripts.secex_new.format_raw_data \
    data/secex/export/MDIC_2004.csv.zip \
    data/secex/import/MDIC_2004.csv.zip \
    -y 2004 \
    -e data/secex/observatory_ecis.csv \
    -p data/secex/observatory_pcis.csv \
    -o data/secex/2004 \
    -g data/secex/2003 \
    -d

"""

''' Import statements '''
import os, sys, time, bz2, click
import pandas as pd
import pandas.io.sql as sql
import numpy as np

from ..growth_lib import growth

from _to_df import to_df
from _replace_vals import replace_vals
from _merge import merge
from _aggregate import aggregate
from _shard import shard
from _pci_wld_eci import pci_wld_eci
from _domestic_eci import domestic_eci
from _calc_diversity import calc_diversity
from _brazil_rca import brazil_rca
from _rdo import rdo
from _growth import calc_growth
from _column_lengths import add_column_length

@click.command()
@click.argument('export_file_path', type=click.Path(exists=True))
@click.argument('import_file_path', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('eci_file_path', '--eci', '-e', help='ECI file.', type=click.Path(exists=True), required=True, prompt="Path to ECI file")
@click.option('pci_file_path', '--pci', '-p', help='PCI file.', type=click.Path(exists=True), required=True, prompt="Path to PCI file")
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
@click.option('prev_path', '--prev', '-g', help='Path to files from the previous year for calculating growth.', type=click.Path(exists=True), required=False)
@click.option('prev5_path', '--prev5', '-g5', help='Path to files from 5 years ago for calculating growth.', type=click.Path(exists=True), required=False)
@click.option('-d', '--debug', is_flag=True)
def main(export_file_path, import_file_path, year, eci_file_path, pci_file_path, output_path, prev_path, prev5_path, debug):
    
    if debug:
        print; print '''STEP 1: \nImport file to pandas dataframe'''
    secex_exports = to_df(export_file_path, False, debug)
    secex_imports = to_df(import_file_path, False, debug)
    # secex_exports = secex_exports.head(1000)
    # secex_imports = secex_imports.head(1000)
    
    if debug:
        print; print '''STEP 2: \nMerge imports and exports'''
    secex_df = merge(secex_imports, secex_exports, debug)
    
    if debug:
        print; print '''STEP 3: \nReplace vals with DB IDs'''
    secex_df = replace_vals(secex_df, None, debug)
    
    if debug:
        print; print '''STEP 4: \nAggregate'''
    ymbpw = aggregate(secex_df)
    
    if debug:
        print; print '''STEP 5: \nShard'''
    [ymb, ymbp, ymbw, ymp, ympw, ymw] = shard(ymbpw)
    
    if debug:
        print; print '''STEP 6: \nCalculate PCI & ECI'''
    [ymp, ymw] = pci_wld_eci(eci_file_path, pci_file_path, ymp, ymw)

    if debug:
        print; print '''STEP 7: \nCalculate domestic ECI'''
    ymb = domestic_eci(ymp, ymb, ymbp)
    
    if debug:
        print; print '''STEP 8: \nCalculate diversity'''
    ymb = calc_diversity(ymbp, ymb, "bra_id", "hs_id")
    ymb = calc_diversity(ymbw, ymb, "bra_id", "wld_id")
    ymp = calc_diversity(ymbp, ymp, "hs_id", "bra_id")
    ymp = calc_diversity(ympw, ymp, "hs_id", "wld_id")
    ymw = calc_diversity(ymbw, ymw, "wld_id", "bra_id")
    ymw = calc_diversity(ympw, ymw, "wld_id", "hs_id")
    
    if debug:
        print; print '''STEP 9: \nCalculate Brazilian RCA'''
    ymp = brazil_rca(ymp, year)
    
    if debug:
        print; print '''STEP 10: \nCalculate RCA, diversity and opp_gain aka RDO'''
    ymbp = rdo(ymbp, ymp, year)
    
    tables = {"ymb": ymb, "ymp": ymp, "ymw": ymw, "ymbp": ymbp, "ymbpw": ymbpw, "ymbw": ymbw, "ympw": ympw}
    

    if prev_path:
        if debug:
            print; print '''STEP 11: \nCalculate 1 year growth'''
        for t_name, t in tables.items():
            prev_file = os.path.join(prev_path, "{0}.tsv.bz2".format(t_name))
            t_prev = to_df(prev_file, t_name)
            t_prev = t_prev.reset_index(level="year")
            t_prev["year"] = int(year)
            t_prev = t_prev.set_index("year", append=True)
            t_prev = t_prev.reorder_levels(["year"] + list(t_prev.index.names)[:-1])
            
            t = calc_growth(t, t_prev)
            
            if prev5_path:
                if debug:
                    print; print '''STEP 12: \nCalculate 5 year growth'''
                prev_file = os.path.join(prev5_path, "{0}.tsv.bz2".format(t_name))
                t_prev = to_df(prev_file, t_name)
                t_prev = t_prev.reset_index(level="year")
                t_prev["year"] = int(year)
                t_prev = t_prev.set_index("year", append=True)
                t_prev = t_prev.reorder_levels(["year"] + list(t_prev.index.names)[:-1])
                
                t_prev = to_df(prev_file, t_name)
                t = calc_growth(t, t_prev, 5)

    if debug:
        print "computing column lengths"
    for table_name, table_data in tables.items():
        table_data = add_column_length(table_name, table_data)

    if debug:
        print; print '''FINAL STEP: \nSave files to output path'''
    for t_name, t in tables.items():
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        new_file_path = os.path.abspath(os.path.join(output_path, "{0}.tsv.bz2".format(t_name)))
        t.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True)

if __name__ == "__main__":
    start = time.time()

    main()
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;