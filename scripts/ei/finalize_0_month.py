#finalize_0_month.py
import pandas as pd
import numpy as np
import os, click, fnmatch
from _helper_columns import add_helper_cols


# via http://stackoverflow.com/questions/13299731/python-need-to-loop-through-directories-looking-for-txt-files
def findFiles (path, filter):
    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, filter):
            yield os.path.join(root, file)

def parse_table(t):
    m = pattern.search(t)
    if m:
        return m.group(1)
    m = pattern2.search(t)
    return m.group(1)


def lookup(tname, with_month=False):
    mylist = []
    for l in tname:
        if l in ['s', 'r']:
            mylist.append('bra_id_'+l)
            mylist.append('cnae_id_'+l)
        elif l == 'p':
            mylist.append('hs_id')
        elif l == 'y':
            mylist.append('year')
        elif l == 'm' and with_month:
            mylist.append('month')
    return mylist

@click.command()
@click.option('--idir', default='.', prompt=False,
              help='Directory for script output.')
def main(idir):
    print "Reading data..."

    # remove month from primary key and sum

    
    table_names = ["ymp", "ymr", "ymrp", "yms", "ymsp", "ymsr", "ymsrp"]
    for table_name in table_names:
        master_frame = pd.DataFrame()
        print table_name

        for f in findFiles(idir, 'output_%s_*.csv' % table_name):
            print f, "FILE"
            if "00.csv" in f:
                print " ** SKIPPING POTENTIAL AGGREGATION MONTH", f
                continue
            ei_df = pd.read_csv(f, header=0, sep=";", quotechar="'")        
            master_frame = pd.concat([master_frame, ei_df])

        pk = lookup(table_name, with_month=False)
        print master_frame.head()
        
        yearly = master_frame.groupby(pk).agg(np.sum)
        yearly["month"] = "00"
        # yearly["bra_id_s"]
        yearly = yearly.set_index("month", append=True)
        yearly = yearly.reorder_levels(lookup(table_name, with_month=True))

        yearly = add_helper_cols(table_name, yearly)

        # write out a zero month file
        output_path = os.path.join(idir,"output_%s_2013_00.csv" % table_name)
        yearly.to_csv(output_path, ";")

if __name__ == '__main__':
    main()