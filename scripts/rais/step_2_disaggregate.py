# -*- coding: utf-8 -*-
"""
    Disaggregate YBIO to subsequent aggregate table
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the second step in adding a new year of RAIS data to the 
    database. The script will output 6 bzipped TSV files that can then be 
    used throughout the rest of the steps.
    
    Like many of the other scripts, the user needs to specify the path to the 
    working directory they will be using that will contain the folder with
    the year they are looking to use including a ybio.tsv[.bz2] file. The year
    also needs to be specified.
    
"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, bz2, gzip, zipfile, time
from collections import defaultdict
from os import environ
from decimal import Decimal, ROUND_HALF_UP
from ..config import DATA_DIR
from ..helpers import d, get_file
from scripts import YEAR

def write(tables, directory, year):
    
    vals = ["wage", "num_emp", "num_est", "wage_avg", "num_emp_est"]
    index_lookup = {"b":"bra_id", "i":"isic_id", "o":"cbo_id", "y": "year"}
    
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
                    d(tables[tbl][var]['wage']), \
                    int(tables[tbl][var]['num_emp']), \
                    int(tables[tbl][var]['num_est']), \
                    d(tables[tbl][var]['wage_avg']), \
                    tables[tbl][var]['num_emp_est'] ])
        
        elif len(tbl) == 3:
                        
            for var1 in tables[tbl].keys():
                for var2 in tables[tbl][var1].keys():
                    new_file_writer.writerow([year, var1, var2, \
                        d(tables[tbl][var1][var2]['wage']), \
                        int(tables[tbl][var1][var2]['num_emp']), \
                        int(tables[tbl][var1][var2]['num_est']), \
                        d(tables[tbl][var1][var2]['wage_avg']), \
                        tables[tbl][var1][var2]['num_emp_est'] ])

def main(year):
    tables = {
        "yb": defaultdict(lambda: defaultdict(float)),
        "ybi": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "ybo": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "yi": defaultdict(lambda: defaultdict(float)),
        "yio": defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
        "yo": defaultdict(lambda: defaultdict(float))
    }
    directory = os.path.abspath(os.path.join(DATA_DIR, "rais"))
    
    '''Open CSV file'''
    ybio_file_path = os.path.abspath(os.path.join(DATA_DIR, 'rais', year, 'ybio.tsv'))
    ybio_file = get_file(ybio_file_path)
    ybio_file_reader = csv.reader(ybio_file, delimiter="\t", quotechar='"')
    header = [s.replace('\xef\xbb\xbf', '') for s in ybio_file_reader.next()]
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(ybio_file_reader):
        
        line = dict(zip(header, line))
        
        if i % 100000 == 0:
            sys.stdout.write('\r lines read: ' + '{:,}'.format(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        if len(line["bra_id"]) == 8 and len(line["isic_id"]) == 5:
            tables["yo"][line["cbo_id"]]["wage"] += float(line["wage"])
            tables["yo"][line["cbo_id"]]["num_emp"] += int(float(line["num_emp"]))
            tables["yo"][line["cbo_id"]]["num_est"] += int(float(line["num_est"]))
            tables["yo"][line["cbo_id"]]["wage_avg"] = tables["yo"][line["cbo_id"]]["wage"] / tables["yo"][line["cbo_id"]]["num_emp"]
            tables["yo"][line["cbo_id"]]["num_emp_est"] = float(tables["yo"][line["cbo_id"]]["num_emp"]) / tables["yo"][line["cbo_id"]]["num_est"]
        if len(line["bra_id"]) == 8 and len(line["cbo_id"]) == 4:
            tables["yi"][line["isic_id"]]["wage"] += float(line["wage"])
            tables["yi"][line["isic_id"]]["num_emp"] += int(float(line["num_emp"]))
            tables["yi"][line["isic_id"]]["num_est"] += int(float(line["num_est"]))
            tables["yi"][line["isic_id"]]["wage_avg"] = tables["yi"][line["isic_id"]]["wage"] / tables["yi"][line["isic_id"]]["num_emp"]
            tables["yi"][line["isic_id"]]["num_emp_est"] = float(tables["yi"][line["isic_id"]]["num_emp"]) / tables["yi"][line["isic_id"]]["num_est"]
        if len(line["isic_id"]) == 5 and len(line["cbo_id"]) == 4:
            tables["yb"][line["bra_id"]]["wage"] += float(line["wage"])
            tables["yb"][line["bra_id"]]["num_emp"] += int(float(line["num_emp"]))
            tables["yb"][line["bra_id"]]["num_est"] += int(float(line["num_est"]))
            tables["yb"][line["bra_id"]]["wage_avg"] = tables["yb"][line["bra_id"]]["wage"] / tables["yb"][line["bra_id"]]["num_emp"]
            tables["yb"][line["bra_id"]]["num_emp_est"] = float(tables["yb"][line["bra_id"]]["num_emp"]) / tables["yb"][line["bra_id"]]["num_est"]

        if len(line["isic_id"]) == 5:
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["wage"] += float(line["wage"])
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp"] += int(float(line["num_emp"]))
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_est"] += int(float(line["num_est"]))
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["wage_avg"] = tables["ybo"][line["bra_id"]][line["cbo_id"]]["wage"] / tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp"]
            tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp_est"] = float(tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_emp"]) / tables["ybo"][line["bra_id"]][line["cbo_id"]]["num_est"]
        if len(line["cbo_id"]) == 4:
            tables["ybi"][line["bra_id"]][line["isic_id"]]["wage"] += float(line["wage"])
            tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp"] += int(float(line["num_emp"]))
            tables["ybi"][line["bra_id"]][line["isic_id"]]["num_est"] += int(float(line["num_est"]))
            tables["ybi"][line["bra_id"]][line["isic_id"]]["wage_avg"] = tables["ybi"][line["bra_id"]][line["isic_id"]]["wage"] / tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp"]
            tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp_est"] = float(tables["ybi"][line["bra_id"]][line["isic_id"]]["num_emp"]) / tables["ybi"][line["bra_id"]][line["isic_id"]]["num_est"]
        if len(line["bra_id"]) == 8:
            tables["yio"][line["isic_id"]][line["cbo_id"]]["wage"] += float(line["wage"])
            tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp"] += int(float(line["num_emp"]))
            tables["yio"][line["isic_id"]][line["cbo_id"]]["num_est"] += int(float(line["num_est"]))
            tables["yio"][line["isic_id"]][line["cbo_id"]]["wage_avg"] = tables["yio"][line["isic_id"]][line["cbo_id"]]["wage"] / tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp"]
            tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp_est"] = float(tables["yio"][line["isic_id"]][line["cbo_id"]]["num_emp"]) / tables["yio"][line["isic_id"]][line["cbo_id"]]["num_est"]
    
    print
    write(tables, directory, year)

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;