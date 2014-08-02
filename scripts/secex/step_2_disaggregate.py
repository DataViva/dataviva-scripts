# -*- coding: utf-8 -*-
"""

    How to run this:
    python -m scripts.secex.step_2_disaggregate \
        -y 2000 \
        data/secex/export/2000/ybpw.tsv.bz2
    
    * you can also pass an optional second argument of the path for the output
      files to be created in.

"""

''' Import statements '''
import csv, sys, os, bz2, time, click
import pandas as pd
from collections import defaultdict
from ..helpers import d, get_file, format_runtime

@click.command()
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('output_dir', type=click.Path(exists=True), required=False)
def disaggregate(year, input_file, output_dir):
    
    '''Open CSV file'''
    click.echo(click.format_filename(input_file))
    ybpw_file = get_file(input_file)
    
    ybpw = pd.read_csv(ybpw_file, sep="\t", converters={"hs_id":str})
    
    yp = ybpw[(ybpw.bra_id.map(lambda x: len(x) == 8)) & (ybpw.wld_id.map(lambda x: len(x) == 5))]
    yp = yp.groupby(['year','hs_id']).sum()
    
    yb = ybpw[(ybpw.hs_id.map(lambda x: len(x) == 6)) & (ybpw.wld_id.map(lambda x: len(x) == 5))]
    yb = yb.groupby(['year','bra_id']).sum()
    
    yw = ybpw[(ybpw.bra_id.map(lambda x: len(x) == 8)) & (ybpw.hs_id.map(lambda x: len(x) == 6))]
    yw = yw.groupby(['year','wld_id']).sum()
    
    
    ybp = ybpw[ybpw.wld_id.map(lambda x: len(x) == 5)]
    ybp = ybp.groupby(['year','bra_id','hs_id']).sum()
   
    ybw = ybpw[ybpw.hs_id.map(lambda x: len(x) == 6)]
    ybw = ybw.groupby(['year','bra_id','wld_id']).sum()
   
    ypw = ybpw[ybpw.bra_id.map(lambda x: len(x) == 8)]
    ypw = ypw.groupby(['year','hs_id','wld_id']).sum()
    
    ybpw = ybpw.set_index(["year", "bra_id", "hs_id", "wld_id"])
    
    yp.to_csv(bz2.BZ2File(os.path.abspath(os.path.join(output_dir, "yp.tsv.bz2")), 'wb'), sep="\t", index=True)
    yb.to_csv(bz2.BZ2File(os.path.abspath(os.path.join(output_dir, "yb.tsv.bz2")), 'wb'), sep="\t", index=True)
    yw.to_csv(bz2.BZ2File(os.path.abspath(os.path.join(output_dir, "yw.tsv.bz2")), 'wb'), sep="\t", index=True)
    ybp.to_csv(bz2.BZ2File(os.path.abspath(os.path.join(output_dir, "ybp.tsv.bz2")), 'wb'), sep="\t", index=True)
    ybw.to_csv(bz2.BZ2File(os.path.abspath(os.path.join(output_dir, "ybw.tsv.bz2")), 'wb'), sep="\t", index=True)
    ypw.to_csv(bz2.BZ2File(os.path.abspath(os.path.join(output_dir, "ypw.tsv.bz2")), 'wb'), sep="\t", index=True)
    ybpw.to_csv(bz2.BZ2File(os.path.abspath(os.path.join(output_dir, "ybpw.tsv.bz2")), 'wb'), sep="\t", index=True)

if __name__ == "__main__":
    start = time.time()
    
    disaggregate()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;