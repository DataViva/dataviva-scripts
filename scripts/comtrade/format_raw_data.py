# -*- coding: utf-8 -*-
"""
    Format BACI data for DB entry
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python scripts/comtrade/format_raw_data.py data/baci/baci92_2013.rar -y 2013 -o data/comtrade/

"""

''' Import statements '''
import os, sys, time, click, bz2
import pandas as pd
import numpy as np
from helpers.import_file import import_file
from helpers.calc_rca import calc_rca
from helpers.calc_complexity import calc_complexity

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../../lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

@click.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True, type=int)
@click.option('-o', '--output_dir', help='output directory', type=click.Path(), default="data/comtrade")
def main(input_file, year, output_dir):
    
    output_dir = os.path.abspath(os.path.join(output_dir, str(year)))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    store = pd.HDFStore(os.path.join(output_dir,'yodp.h5'))
    
    try:
        ypw = store.get('ypw')
    except KeyError:
        '''
        Import file to pandas dataframe
        '''
        comtrade_df = import_file(input_file)
    
        '''
        Add indexes
        '''
        ypw = comtrade_df.groupby(['hs_id', 'wld_id']).sum()
        
        store.put('ypw', ypw)
    
    '''
    Calculate RCA
    '''
    ypw_rca = ypw.reset_index()
    ypw_rca = ypw_rca.pivot(index="wld_id", columns="hs_id", values="val_usd")
    ypw_rca = ps_calcs.rca(ypw_rca)
    
    ypw_rca_binary = ypw_rca.copy()
    ypw_rca_binary[ypw_rca_binary >= 1] = 1
    ypw_rca_binary[ypw_rca_binary < 1] = 0
    
    '''
        DISTANCES
    '''
    ypw_prox = ps_calcs.proximity(ypw_rca_binary)
    ypw_dist = ps_calcs.distance(ypw_rca_binary, ypw_prox).fillna(0)
    
    '''
        COMPLEXITY
    '''
    eci, pci = calc_complexity(ypw)
    
    '''
        OPP GAIN
    '''
    ypw_opp_gain = ps_calcs.opportunity_gain(ypw_rca_binary[pci.index], ypw_prox[pci.index].reindex(pci.index), pci)
    
    '''
        MERGE DATA
    '''
    ypw_opp_gain = pd.DataFrame(ypw_opp_gain.T.stack(), columns=["opp_gain"])
    ypw_opp_gain = ypw_opp_gain.replace(0, np.nan)
    
    ypw_dist = pd.DataFrame(ypw_dist.T.stack(), columns=["distance"])
    ypw_dist = ypw_dist.replace(0, np.nan)
    
    ypw_rca = pd.DataFrame(ypw_rca.T.stack(), columns=["rca"])
    ypw_rca = ypw_rca.replace(0, np.nan)
    
    new_ypw = ypw \
                .merge(ypw_rca, how="outer", left_index=True, right_index=True) \
                .merge(ypw_dist, how="outer", left_index=True, right_index=True) \
                .merge(ypw_opp_gain, how="outer", left_index=True, right_index=True)
    new_ypw = new_ypw.reset_index()
    new_ypw["year"] = year
    cols = new_ypw.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    new_ypw = new_ypw[cols]
    
    
    '''
    Write out to files
    '''
    new_file_path = os.path.abspath(os.path.join(output_dir, "comtrade_ypw.tsv.bz2"))
    new_ypw.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=False, float_format="%.3f")
    
    new_file_path = os.path.abspath(os.path.join(output_dir, "comtrade_pci.tsv.bz2"))
    pd.DataFrame(pci, columns=["pci"]).to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.3f")
    
    new_file_path = os.path.abspath(os.path.join(output_dir, "comtrade_eci.tsv.bz2"))
    pd.DataFrame(eci, columns=["eci"]).to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.3f")
    

if __name__ == "__main__":
    main()