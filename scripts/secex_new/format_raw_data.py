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
    data/secex/import/MDIC_2001.rar \
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
from _val_per_unit import val_per_unit
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
def main(export_file_path, import_file_path, year, eci_file_path, pci_file_path, output_path, prev_path, prev5_path):
    step = 0
    # d = pd.HDFStore(os.path.abspath(os.path.join(output_path,'ymp.h5')))
    
    step += 1; print '''\nSTEP {0}: \nImport file to pandas dataframe'''.format(step)
    secex_exports = to_df(export_file_path, False)
    secex_imports = to_df(import_file_path, False)
    # secex_exports = secex_exports.head(1000)
    # secex_imports = secex_imports.head(1000)

    step += 1; print '''\nSTEP {0}: \nMerge imports and exports'''.format(step)
    secex_df = merge(secex_exports, secex_imports)

    step += 1; print '''\nSTEP {0}: \nAggregate'''.format(step)
    ymbpw = aggregate(secex_df)

    step += 1; print '''\nSTEP {0}: \nShard'''.format(step)
    [ymb, ymbp, ymbw, ymp, ympw, ymw] = shard(ymbpw)

    step += 1; print '''\nSTEP {0}: \nPrice / unit'''.format(step)
    ymp = val_per_unit(ymp, secex_exports, "export")
    ymp = val_per_unit(ymp, secex_imports, "import")

    step += 1; print '''\nSTEP {0}: \nCalculate PCI & ECI'''.format(step)
    [ymp, ymw] = pci_wld_eci(eci_file_path, pci_file_path, ymp, ymw)

    step += 1; print '''\nSTEP {0}: \nCalculate domestic ECI'''.format(step)
    ymb = domestic_eci(ymp, ymb, ymbp)

    step += 1; print '''\nSTEP {0}: \nCalculate diversity'''.format(step)
    ymb = calc_diversity(ymbp, ymb, "bra_id", "hs_id")
    ymb = calc_diversity(ymbw, ymb, "bra_id", "wld_id")
    ymp = calc_diversity(ymbp, ymp, "hs_id", "bra_id")
    ymp = calc_diversity(ympw, ymp, "hs_id", "wld_id")
    ymw = calc_diversity(ymbw, ymw, "wld_id", "bra_id")
    ymw = calc_diversity(ympw, ymw, "wld_id", "hs_id")

    step += 1; print '''\nSTEP {0}: \nCalculate Brazilian RCA'''.format(step)
    ymp = brazil_rca(ymp, year)
    
    # d["ymp"] = ymp
    # d["ymbp"] = ymbp
    # sys.exit()
    # ymp = d["ymp"]
    # ymbp = d["ymbp"]
    
    step += 1; print '''\nSTEP {0}: \nCalculate RCA, diversity and opp_gain aka RDO'''.format(step)
    ymbp = rdo(ymbp, ymp, year)
    # ymbp.to_csv('ymbp_temp.csv')
    # print ymbp.head(); sys.exit();
    
    tables = {"ymb": ymb, "ymp": ymp, "ymw": ymw, "ymbp": ymbp, "ymbpw": ymbpw, "ymbw": ymbw, "ympw": ympw}
    
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
                
                t_prev = to_df(prev_file, t_name)
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