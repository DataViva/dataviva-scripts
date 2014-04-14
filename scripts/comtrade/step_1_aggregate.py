# -*- coding: utf-8 -*-
"""
    Clean raw SECEX data and output to TSV
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the first step in adding a new year of SECEX data to the 
    database. The script will output 1 bzipped TSV file that can then be 
    consumed by step 2 for created the disaggregate tables.

"""


''' Import statements '''
import csv, sys, os, argparse, MySQLdb, time, bz2
from collections import defaultdict
from os import environ
from decimal import Decimal, ROUND_HALF_UP
from ..config import DATA_DIR
from ..helpers import d, get_file
from scripts import YEAR

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=environ["DATAVIVA_DB_USER"], 
                        passwd=environ["DATAVIVA_DB_PW"], 
                        db=environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def hs_format(hs_code, lookup):
    # make sure it's a 6 digit (with leading zeros)
    # hs_code = hs_code.zfill(6)
    # take off last 2 digits
    # hs_code = hs_code[:-2]
    if hs_code in ['9991', '9992', '9998', '9997']:
        return "229999"
    return lookup[hs_code]

def wld_format(wld, lookup):
    if wld == "695":
        return "nakna"
    elif wld == "423":
        return "asmys"
    elif wld == "152":
        return "euchi"
    return lookup[int(wld)]

def get_lookup(type):
    if type == "hs":
        cursor.execute("select id from attrs_hs where length(id)=6")
        return {r[0][2:]:r[0] for r in cursor.fetchall()}
    elif type == "wld":
        cursor.execute("select id_num, id from attrs_wld where id_num is not null")
        return {r[0]:r[1] for r in cursor.fetchall()}

def main(year):
    '''Initialize our data dictionaries'''
    ywpw = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    
    lookup = {"hs": get_lookup("hs"), "wld": get_lookup("wld"), "wld_partner": get_lookup("wld")}
    
    var_names = {"hs": ["Commodity Code", hs_format], "val_usd": ["Value", float], \
                    "wld": ["Reporter Code", wld_format], \
                    "wld_partner":["Partner Code", wld_format]}
    
    '''Open CSV file'''
    raw_file_path = os.path.abspath(os.path.join(DATA_DIR, 'comtrade', 'Comtrade_{0}.csv'.format(year)))
    raw_file = get_file(raw_file_path)
    delim = ","
    
    csv_reader = csv.reader(raw_file, delimiter=delim)
    
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    errors_dict = defaultdict(set)
    
    non_countries = ['0', '490', '899', '531', '568', '536', '837', '260', '577', '839', '534', '838', '527', '473', '637', '535']
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(csv_reader):
        
        line = dict(zip(header, line))
        # print wld_format(line['Partner Code'], lookup["wld"])
        # sys.exit()
        
        if i % 100000 == 0:
            sys.stdout.write('\r lines read: ' + str(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        data = var_names.copy()
        errors = False
        
        for var, var_name in data.items():
            
            formatter = None
            if isinstance(var_name, list):
                var_name, formatter = var_name
            
            if var == "hs" and len(line[var_name]) != 4:
                errors = True
                continue
            if ("wld_partner" and line[var_name] in non_countries) or ("wld" and line[var_name] in non_countries):
                errors = True
                continue
            
            try:
                data[var] = line[var_name].strip()
            except KeyError:
                print
                # print "Error reading year on line {0}".format(i+1)
                new_col = raw_input('Could not find value for "{0}" column. '\
                                'Use different column name? : ' \
                                .format(var_name))
                if isinstance(var_names[var], list):
                    var_names[var][0] = new_col
                else:
                    var_names[var] = new_col
                try:
                    data[var] = line[new_col].strip()
                except KeyError:
                    errors = True
                    continue
            
            # run proper formatting
            if formatter:
                try:
                    if var in lookup:
                        data[var] = formatter(data[var], lookup[var])
                    else:
                        data[var] = formatter(data[var])
                except:
                    print line
                    # print formatter(data[var], lookup[var])
                    print "Error reading {0} ID on line {1}. Got value: '{2}'".format(var, i+1, data[var])
                    errors_dict[var].add(data[var])
                    errors = True
                    sys.exit()
                    continue
        
        if errors:
            continue
        
        # print data
        # sys.exit()
        
        ywpw[data["wld"]][data["hs"]][data["wld_partner"]] += data["val_usd"]
        
    print errors_dict
    
    columns = {"y":"year", "b":"bra_id", "i":"isic_id", "o":"cbo_id"}
    
    print
    print "finished reading file, writing output..."
    
    comtrade_dir = os.path.abspath(os.path.join(DATA_DIR, 'comtrade'))
    if not os.path.exists(comtrade_dir):
        os.makedirs(comtrade_dir)
    new_dir = os.path.abspath(os.path.join(DATA_DIR, 'comtrade', year))
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)
    
    new_file = os.path.abspath(os.path.join(new_dir, "ywpw.tsv.bz2"))
    print ' writing file: ', new_file
    
    '''Create header for CSV file'''
    header = ["year", "wld_id", "hs_id", "wld_id_partner", "val_usd"]
    
    '''Export to files'''
    csv_writer = csv.writer(bz2.BZ2File(new_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(header)
    
    for wld in ywpw.keys():
        for hs in ywpw[wld].keys():
            for wld_p in ywpw[wld][hs].keys():
                csv_writer.writerow([year, wld, hs, wld_p, d(ywpw[wld][hs][wld_p]) ])

if __name__ == "__main__":
    start = time.time()
    
    main(YEAR)
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;