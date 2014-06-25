# -*- coding: utf-8 -*-
"""
    Clean raw SECEX data and output to TSV
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the first step in adding a new year of SECEX data to the 
    database. The script will output 1 bzipped TSV file that can then be 
    consumed by step 2 for creating the disaggregate tables.
    
    How to run this: 
    python -m scripts.secex.step_1_aggregate -y 2000 /path/to/data/secex/export/MDIC_2000.csv.zip
    
    * you can also pass an optional second argument of the path for the output
      file. This output file should end in .bz2 as the output is always bzipped.

"""


''' Import statements '''
import csv, sys, os, MySQLdb, time, bz2, click
from collections import defaultdict
from ..config import DATA_DIR
from ..helpers import d, get_file, format_runtime

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def hs_format(hs_code, lookup):
    if len(hs_code)>4:
        # take off last 2 digits
        hs_code = hs_code[:-2]
   
    # make sure it's a 4 digit (with leading zeros). Ex.: 10119 will be 010119
    hs_code = hs_code.zfill(4)
    
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

def munic_format(munic, lookup):
    if munic == "9999999":
        return "xx000007"
    elif munic == "9400000":
        return "xx000002"
    try:
        return lookup[munic]
    except:
        print munic
        sys.exit()

def get_lookup(type):
    if type == "bra":
        cursor.execute("select id_mdic, id from attrs_bra where length(id)=8 or length(id)=2")
        return {str(r[0]):r[1] for r in cursor.fetchall()}
    elif type == "hs":
        #Example: Original number 01010119 will be 010119
        cursor.execute("select id from attrs_hs")
        return {r[0][2:]:r[0] for r in cursor.fetchall()}
    elif type == "wld":
        cursor.execute("select id_mdic, id from attrs_wld")
        return {r[0]:r[1] for r in cursor.fetchall()}
    elif type == "pr":
        cursor.execute("select bra_id, pr_id from attrs_bra_pr")
        return {r[0]:r[1] for r in cursor.fetchall()}

def add(ybpw, munic, isic, occ, val_usd):
    ybpw[munic][isic][occ]["val_usd"] += val_usd
    return ybpw

@click.command()
@click.option('--year', '-y', help='The year of the data.', type=click.INT, required=True)
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('output_file', type=click.File('wb'), required=False)
def aggregate(year, input_file, output_file):
    
    if not output_file:
        dirname = os.path.dirname(input_file)
        year_dir = os.path.abspath(os.path.join(dirname, str(year)))
        if not os.path.exists(year_dir):
            os.makedirs(year_dir)
        output_file = os.path.abspath(os.path.join(year_dir, "ybpw.tsv.bz2"))
    
    csv_output = csv.writer(bz2.BZ2File(output_file, 'wb'), delimiter='\t',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    header = ["year", "bra_id", "hs_id", "wld_id", "val_usd"]
    csv_output.writerow(header)
    
    '''Initialize our data dictionaries'''
    ybpw = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float))))
    
    lookup = {"state": get_lookup("bra"), "munic": get_lookup("bra"), "hs": get_lookup("hs"), \
                "wld": get_lookup("wld"), "pr": get_lookup("pr")}
    
    var_names = {"year":["Year", int], "munic": ["Municipality_ID", munic_format], \
                    "state": ["State_ID", munic_format], \
                    "hs": ["TransactedProduct_ID_HS", hs_format], \
                    "wld": ["DestinationCoutnry_ID", wld_format], \
                    "val_usd":["TransactionAmount_US$_FOB", float]}
    
    '''Open CSV file'''
    click.echo(click.format_filename(input_file))
    input_file = get_file(input_file)
    
    delim = "|"
    csv_reader = csv.reader(input_file, delimiter=delim)
    
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    errors_dict = defaultdict(set)
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(csv_reader):
        
        line = dict(zip(header, line))
        
        if i % 100000 == 0:
            sys.stdout.write('\r lines read: ' + str(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        data = var_names.copy()
        errors = False
        
        for var, var_name in data.items():
            formatter = None
            if isinstance(var_name, list):
                var_name, formatter = var_name
            
            try:
                data[var] = line[var_name].strip()
            except KeyError:
                print
                # print "Error reading year on line {0}".format(i+1)
                if var_name == "EconomicAtivity_ID_ISIC":
                    new_col = "EconmicAtivity_ID_ISIC"
                else:
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
                    print "Error reading {0} ID on line {1}. Got value: '{2}'".format(var, i+1, data[var])
                    errors_dict[var].add(data[var])
                    errors = True
                    #sys.exit()
                    continue
        
        if errors:
            continue
        
        ybpw = add(ybpw, data["munic"], data["hs"], data["wld"], data["val_usd"])

        # wld 2digit (continents)
        ybpw = add(ybpw, data["munic"], data["hs"], data["wld"][:2], data["val_usd"])
        
        # hs 2digit
        ybpw = add(ybpw, data["munic"], data["hs"][:2], data["wld"][:2], data["val_usd"])
        ybpw = add(ybpw, data["munic"], data["hs"][:2], data["wld"], data["val_usd"])
        # hs 4digit
        ybpw = add(ybpw, data["munic"], data["hs"][:4], data["wld"][:2], data["val_usd"])
        ybpw = add(ybpw, data["munic"], data["hs"][:4], data["wld"], data["val_usd"])
        
        # bra 4digit
        ybpw = add(ybpw, data["munic"][:4], data["hs"][:2], data["wld"][:2], data["val_usd"])
        ybpw = add(ybpw, data["munic"][:4], data["hs"][:4], data["wld"][:2], data["val_usd"])
        ybpw = add(ybpw, data["munic"][:4], data["hs"][:2], data["wld"], data["val_usd"])
        ybpw = add(ybpw, data["munic"][:4], data["hs"][:4], data["wld"], data["val_usd"])
        ybpw = add(ybpw, data["munic"][:4], data["hs"], data["wld"][:2], data["val_usd"])
        ybpw = add(ybpw, data["munic"][:4], data["hs"], data["wld"], data["val_usd"])
        
        # bra 2digit
        ybpw = add(ybpw, data["state"], data["hs"][:2], data["wld"][:2], data["val_usd"])
        ybpw = add(ybpw, data["state"], data["hs"][:4], data["wld"][:2], data["val_usd"])
        ybpw = add(ybpw, data["state"], data["hs"][:2], data["wld"], data["val_usd"])
        ybpw = add(ybpw, data["state"], data["hs"][:4], data["wld"], data["val_usd"])
        ybpw = add(ybpw, data["state"], data["hs"], data["wld"][:2], data["val_usd"])
        ybpw = add(ybpw, data["state"], data["hs"], data["wld"], data["val_usd"])
        
        if data["munic"] in lookup["pr"]:
            ybpw = add(ybpw, lookup["pr"][data["munic"]], data["hs"][:2], data["wld"][:2], data["val_usd"])
            ybpw = add(ybpw, lookup["pr"][data["munic"]], data["hs"][:4], data["wld"][:2], data["val_usd"])
            ybpw = add(ybpw, lookup["pr"][data["munic"]], data["hs"][:2], data["wld"], data["val_usd"])
            ybpw = add(ybpw, lookup["pr"][data["munic"]], data["hs"][:4], data["wld"], data["val_usd"])
            ybpw = add(ybpw, lookup["pr"][data["munic"]], data["hs"], data["wld"][:2], data["val_usd"])
            ybpw = add(ybpw, lookup["pr"][data["munic"]], data["hs"], data["wld"], data["val_usd"])
    
    print
    if errors_dict <> defaultdict(set):
        print "Errors:"
        print errors_dict
    
    columns = {"y":"year", "b":"bra_id", "i":"isic_id", "o":"cbo_id"}
    
    print
    print "finished reading file, writing output..."

    for bra in ybpw.keys():
        for hs in ybpw[bra].keys():
            for wld in ybpw[bra][hs].keys():
                csv_output.writerow([year, bra, hs, wld, d(ybpw[bra][hs][wld]['val_usd']) ])

if __name__ == "__main__":
    start = time.time()
    
    aggregate()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;