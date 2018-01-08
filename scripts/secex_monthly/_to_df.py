import pandas as pd
import rarfile, sys, os, MySQLdb, bz2
from collections import defaultdict

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), 
                     user=os.environ["DATAVIVA_DB_USER"], 
                     passwd=os.environ["DATAVIVA_DB_PW"], 
                     db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

cursor.execute("select id_mdic, id from attrs_bra where id_mdic is not null and length(id) = 9;")
bra_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
bra_lookup["4314548"] = "5rs030014"
bra_lookup["9999999"] = "0xx000007"
bra_lookup["9400000"] = "0xx000002"

cursor.execute("select id_mdic, id from attrs_bra where id_mdic is not null and length(id) = 3;")
state_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
state_lookup["94"] = "0xx"
state_lookup["95"] = "0xx"
state_lookup["96"] = "0xx"
state_lookup["97"] = "0xx"
state_lookup["98"] = "0xx"
state_lookup["99"] = "0xx"

cursor.execute("select substr(id, 3), id from attrs_hs where substr(id, 3) != '' and length(id) = 6;")
hs_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
hs_lookup["9991"] = "229999"
hs_lookup["9992"] = "229999"
hs_lookup["9998"] = "229999"
hs_lookup["9997"] = "229999"

cursor.execute("select id_mdic, id from attrs_wld where id_mdic is not null and length(id) = 5;")
wld_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
wld_lookup["695"] = "nakna"
wld_lookup["423"] = "asmys"
wld_lookup["152"] = "euchi"
wld_lookup["997"] = "xxxxd"

missing = {
    "bra_id": defaultdict(int),
    "state_id": defaultdict(int),
    "hs_id": defaultdict(int),
    "wld_id": defaultdict(int),
    "month": defaultdict(int),
    "val_usd": defaultdict(int),
    "val_kg": defaultdict(int)
}

def bra_replace(raw):
    try: return bra_lookup[str(raw).strip()]
    except: missing["bra_id"][raw] += 1

def state_replace(raw):
    try: return state_lookup[str(raw).strip()]
    except: missing["state_id"][raw] += 1

def hs_replace(raw):
    try: return hs_lookup[str(raw).strip()]
    except: missing["hs_id"][raw] += 1

def wld_replace(raw):
    try: return wld_lookup[str(int(raw))]
    except: missing["wld_id"][raw] += 1

def val_usd(raw):
    try: return int(raw)
    except: missing["val_usd"][raw] += 1

def val_kg(raw):
    try: return int(raw)
    except: missing["val_kg"][raw] += 1

def format_month(raw):
    try: month = int(raw)
    except: missing["month"][raw] += 1
    if month in range(1, 13): return str(month).zfill(2)
    missing["month"][raw] += 1

def to_df(input_file_path, index=False, debug=False):
    if "rar" in input_file_path:
        rar_file = rarfile.RarFile(input_file_path)
        file_name = os.path.basename(input_file_path)
        file_path_no_ext, file_ext = os.path.splitext(file_name)
        input_file = rarfile.RarFile.open(rar_file, file_path_no_ext+".csv")
    elif "bz2" in input_file_path:
        input_file = bz2.BZ2File(input_file_path)
    else:
        input_file = open(input_file_path, "rU")
    
    if index:
        index_lookup = {"y":"year", "m":"month", "b":"bra_id", "p":"hs_id", "w":"wld_id"}
        index_cols = [index_lookup[i] for i in index]
        # secex_df = pd.read_csv(input_file, sep="\t", converters={"month":str, "hs_id":str}, low_memory=False)
        secex_df = pd.read_csv(input_file, sep="\t", converters={"month":str, "hs_id":str}, engine='python')
        secex_df = secex_df.set_index(index_cols)
    else:
        cols = ["year", "month", "wld_id", "state_id", "customs", "bra_id", \
                "val_kg", "val_usd", "hs_id"]
        delim = "|"
        coerce_cols = {"bra_id":bra_replace, "month":format_month, "hs_id":hs_replace, \
                        "state_id":state_replace, "wld_id":wld_replace, "val_kg":val_kg, "val_usd":val_usd}
        secex_df = pd.read_csv(input_file, header=0, sep=delim, converters=coerce_cols, names=cols)
        secex_df = secex_df[["year", "month", "state_id", "bra_id", "hs_id", "wld_id", "val_usd", "val_kg"]]
        
        for col, missings in missing.items():
            if not len(missings): continue
            num_rows = secex_df.shape[0]
            print; print "[WARNING]"; print "The following {0} IDs are not in the DB and will be dropped from the data.".format(col);
            print list(missings)
            secex_df = secex_df.dropna(subset=[col])
            print; print "{0} rows deleted.".format(num_rows - secex_df.shape[0]); print;
    
    return secex_df