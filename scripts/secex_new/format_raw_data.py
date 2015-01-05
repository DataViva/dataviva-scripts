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
    python scripts/secex_new/format_raw_data.py \
    data/secex/export/MDIC_2001.rar \
    -t export \
    -y 2001 \
    -e data/secex/observatory_ecis.csv \
    -p data/secex/observatory_pcis.csv \
    -o data/secex/2001 \
    -g data/secex/2000

"""

''' Import statements '''
import os, sys, time, bz2, click
import pandas as pd
import pandas.io.sql as sql
import numpy as np

from _to_df import to_df
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
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-t', '--trade_flow', prompt='Trade flow', help='direction of trade export or import?', type=click.Choice(['export', 'import']), required=True)
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('eci_file_path', '--eci', '-e', help='ECI file.', type=click.Path(exists=True), required=True, prompt="Path to ECI file")
@click.option('pci_file_path', '--pci', '-p', help='PCI file.', type=click.Path(exists=True), required=True, prompt="Path to PCI file")
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
@click.option('prev_path', '--prev', '-g', help='Path to files from the previous year for calculating growth.', type=click.Path(exists=True), required=False)
@click.option('prev5_path', '--prev5', '-g5', help='Path to files from 5 years ago for calculating growth.', type=click.Path(exists=True), required=False)
def main(file_path, trade_flow, year, eci_file_path, pci_file_path, output_path, prev_path, prev5_path):
    step = 0
    
    depths = {
        "bra": [1, 3, 5, 7, 8, 9],
        "hs": [2, 6],
        "wld": [2, 5]
    }
    
    step += 1; print '''\nSTEP {0}: \nImport file to pandas dataframe'''.format(step)
    secex_df = to_df(file_path, False)
    # secex_df = secex_df.head(1000)

    step += 1; print '''\nSTEP {0}: \nAggregate'''.format(step)
    ybpw = aggregate(secex_df)

    step += 1; print '''\nSTEP {0}: \nShard'''.format(step)
    [yb, ybp, ybw, yp, ypw, yw] = shard(ybpw, depths)

    step += 1; print '''\nSTEP {0}: \nCalculate PCI & ECI'''.format(step)
    if trade_flow == "export":
        [yp, yw] = pci_wld_eci(eci_file_path, pci_file_path, yp, yw)

    step += 1; print '''\nSTEP {0}: \nCalculate domestic ECI'''.format(step)
    yb = domestic_eci(yp, yb, ybp, depths)

    step += 1; print '''\nSTEP {0}: \nCalculate diversity'''.format(step)
    yb = calc_diversity(ybp, yb, "bra_id", "hs_id", depths)
    yb = calc_diversity(ybw, yb, "bra_id", "wld_id", depths)
    yp = calc_diversity(ybp, yp, "hs_id", "bra_id", depths)
    yp = calc_diversity(ypw, yp, "hs_id", "wld_id", depths)
    yw = calc_diversity(ybw, yw, "wld_id", "bra_id", depths)
    yw = calc_diversity(ypw, yw, "wld_id", "hs_id", depths)

    step += 1; print '''\nSTEP {0}: \nCalculate Brazilian RCA'''.format(step)
    yp = brazil_rca(yp, year)
    
    step += 1; print '''\nSTEP {0}: \nCalculate RCA, diversity and opp_gain aka RDO'''.format(step)
    ybp = rdo(ybp, yp, year, depths)
    
    tables = {"yb": yb, "yp": yp, "yw": yw, "ybp": ybp, "ybpw": ybpw, "ybw": ybw, "ypw": ypw}
    
    if prev_path:
        step += 1; print '''\nSTEP {0}: \nCalculate 1 year growth'''.format(step)
        if prev5_path:
            step += 1; print '''\nSTEP {0}: \nCalculate 5 year growth'''.format(step)
        for t_name, t in tables.items():
            prev_file = os.path.join(prev_path, "{0}.tsv.bz2".format(t_name))
            t_prev = to_df(prev_file, t_name)
            t_prev = t_prev.reset_index(level="year")
            t_prev["year"] = int(year)
            t_prev = t_prev.set_index("year", append=True)
            t_prev = t_prev.reorder_levels(["year"] + list(t_prev.index.names)[:-1])
            
            t = calc_growth(t, t_prev)
            
            if prev5_path:
                prev_file = os.path.join(prev5_path, "{0}.tsv.bz2".format(t_name))
                t_prev = to_df(prev_file, t_name)
                t_prev = t_prev.reset_index(level="year")
                t_prev["year"] = int(year)
                t_prev = t_prev.set_index("year", append=True)
                t_prev = t_prev.reorder_levels(["year"] + list(t_prev.index.names)[:-1])
                
                t = calc_growth(t, t_prev, 5)

    print "computing column lengths"
    for table_name, table_data in tables.items():
        tables[table_name] = add_column_length(table_name, table_data)

    print '''\nFINAL STEP: \nSave files to output path'''
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