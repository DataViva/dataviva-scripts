# -*- coding: utf-8 -*-
"""
"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, bz2, time
from collections import defaultdict
from os import environ
from decimal import Decimal, ROUND_HALF_UP
from ..config import DATA_DIR
from ..helpers import d, get_file
from scripts import YEAR

def write(tables, year):
    
    vals = ["val_usd"]
    directory = os.path.abspath(os.path.join(DATA_DIR, 'comtrade'))
    index_lookup = {"b":"bra_id", "p":"hs_id", "w":"wld_id", "y": "year"}
    
    for tbl in tables.keys():
        
        new_file_name = tbl+".tsv.bz2"
        new_file = os.path.abspath(os.path.join(directory, year, new_file_name))
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

def main(year):
    tables = {
        "ypw": defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    }
    
    '''Open CSV file'''
    ywpw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'comtrade', year, 'ywpw.tsv'))
    ywpw_file = get_file(ywpw_file_path)
    
    csv_reader = csv.reader(ywpw_file, delimiter="\t", quotechar='"')
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(csv_reader):
        
        line = dict(zip(header, line))
        
        if i % 100000 == 0:
            sys.stdout.write('\r lines read: ' + str(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        if len(line["wld_id"]) == 5:
            tables["ypw"][line["hs_id"]][line["wld_id"]]["val_usd"] += float(line["val_usd"])
    
    print
    write(tables, year)

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;