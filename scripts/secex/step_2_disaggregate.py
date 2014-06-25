# -*- coding: utf-8 -*-
"""

    How to run this:
    python -m scripts.secex.step_2_disaggregate -y 2000 /path/to/data/secex/export/2000/ybpw.tsv.bz2
    
    * you can also pass an optional second argument of the path for the output
      files to be created in.

"""

''' Import statements '''
import csv, sys, os, bz2, time, click
from collections import defaultdict
from ..helpers import d, get_file, format_runtime

def write(tables, year, output_dir):
    
    vals = ["val_usd"]
    index_lookup = {"b":"bra_id", "p":"hs_id", "w":"wld_id", "y": "year"}
    
    for tbl in tables.keys():
        
        new_file_name = tbl+".tsv.bz2"
        new_file = os.path.abspath(os.path.join(output_dir, new_file_name))
        new_file_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        '''Add headers'''
        variable_cols = [index_lookup[char] for char in tbl]
        new_file_writer.writerow(variable_cols + vals)
        
        print 'writing file:', new_file
        
        if len(tbl) == 2:
            
            for var in tables[tbl].keys():
                new_file_writer.writerow([year, var, \
                    d(tables[tbl][var]['val_usd']) ])
        
        elif len(tbl) == 3:
                        
            for var1 in tables[tbl].keys():
                for var2 in tables[tbl][var1].keys():
                    new_file_writer.writerow([year, var1, var2, \
                        d(tables[tbl][var1][var2]['val_usd']) ])

@click.command()
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('output_dir', type=click.Path(exists=True), required=False)
def disaggregate(year, input_file, output_dir):
    tables = {
        "yb": defaultdict(lambda: defaultdict(float)),
        "yp": defaultdict(lambda: defaultdict(float)),
        "yw": defaultdict(lambda: defaultdict(float)),
        "ybp": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "ybw": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "ypw": defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    }
    
    '''Open CSV file'''
    click.echo(click.format_filename(input_file))
    ybpw_file = get_file(input_file)
    
    delim = "\t"
    csv_reader = csv.reader(ybpw_file, delimiter=delim, quotechar='"')
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(csv_reader):
        
        line = dict(zip(header, line))
        
        if i % 100000 == 0:
            sys.stdout.write('\r lines read: ' + str(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        if len(line["bra_id"]) == 8 and len(line["hs_id"]) == 6:
            tables["yw"][line["wld_id"]]["val_usd"] += float(line["val_usd"])
        if len(line["bra_id"]) == 8 and len(line["wld_id"]) == 5:
            tables["yp"][line["hs_id"]]["val_usd"] += float(line["val_usd"])
        if len(line["hs_id"]) == 6 and len(line["wld_id"]) == 5:
            tables["yb"][line["bra_id"]]["val_usd"] += float(line["val_usd"])

        if len(line["hs_id"]) == 6:
            tables["ybw"][line["bra_id"]][line["wld_id"]]["val_usd"] += float(line["val_usd"])
        if len(line["wld_id"]) == 5:
            tables["ybp"][line["bra_id"]][line["hs_id"]]["val_usd"] += float(line["val_usd"])
        if len(line["bra_id"]) == 8:
            tables["ypw"][line["hs_id"]][line["wld_id"]]["val_usd"] += float(line["val_usd"])
    
    print
    
    if not output_dir:
        output_dir = os.path.dirname(input_file)
    
    write(tables, year, output_dir)

if __name__ == "__main__":
    start = time.time()
    
    disaggregate()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;