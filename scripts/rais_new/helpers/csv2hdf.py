import pandas as pd
import sys, os, MySQLdb
import pandas as pd
import numpy as np
from collections import defaultdict
import click


db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA2_DB_USER"], 
                        passwd=os.environ["DATAVIVA2_DB_PW"], 
                        db=os.environ["DATAVIVA2_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 8;")
bra_lookup = {str(r[0])[:-1]:r[1] for r in cursor.fetchall()}

cursor.execute("select substr(id, 2), id from attrs_cnae where length(id) = 6;")
cnae_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}

cursor.execute("select id, id from attrs_cbo where length(id) = 4;")
cbo_lookup = {r[0]:r[1] for r in cursor.fetchall()}
cbo_lookup["-1"] = "xxxx" # data uses -1 for missing occupation

missing = {
    "bra_id": defaultdict(int),
    "cnae_id": defaultdict(int),
    "cbo_id": defaultdict(int)
}

def map_gender(x):
    MALE, FEMALE = 0, 1
    gender_dict = {MALE: 1, FEMALE: 2}
    if x in gender_dict:
        return str(gender_dict[x])
    return str(3)

def map_color(color):
    INDIAN, WHITE, BLACK, ASIAN, MULTI, UNKNOWN = 1,2,4,6,8,9
    color_dict = {INDIAN:1, WHITE:2, BLACK:3, ASIAN:4, MULTI:5, 9:UNKNOWN, -1:UNKNOWN }
    return str(color_dict[int(color)])

def map_age(age):
    age_bucket = int(np.floor( int(age) / 10 ))
    if age_bucket == 0: 
        age_bucket = 1
    elif age_bucket > 6:
        age_bucket = 6
    return str(age_bucket)

def map_literacy(lit):
    ILLITERATE, BASIC, HIGHSCHOOL, COLLEGE, UNKNOWN = 1, 2, 3, 4, 9
    lit_dict = {1:ILLITERATE, 2:ILLITERATE, 3:BASIC, 4:BASIC, 5:BASIC, 6:BASIC, 7:HIGHSCHOOL, 
                8:HIGHSCHOOL, 9:COLLEGE, -1:UNKNOWN }
    return str(lit_dict[int(lit)])

def floatvert(x):
    x = x.replace(',', '.')
    try:
        return float(x)
    except:
        return np.nan

def bra_replace(raw):
    try:
        return bra_lookup[str(raw).strip()]
    except:
        missing["bra_id"][raw] += 1
        return None

def cnae_replace(raw):
    try:
        return cnae_lookup[str(raw).strip()]
    except:
        missing["cnae_id"][raw] += 1
        return None

def cbo_replace(raw):
    try:
        return cbo_lookup[str(raw).strip()[:4]]
    except:
        missing["cbo_id"][raw] += 1
        return None


cols = ["cbo_id", "cnae_id", "literacy", "age", "est_id", "simple", "bra_id", "num_emp", "color", "wage_dec", "wage", "gender", "est_size", "year"]

coerce_cols = {"bra_id": bra_replace, "cnae_id":cnae_replace, "cbo_id":cbo_replace, \
                       "wage":floatvert, "emp_id":str, "est_id": str}

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
def main(file_path):
    output_file = file_path + ".h5" 
    df = pd.read_csv(file_path, header=0, sep=";", names=cols, converters=coerce_cols)
    df["d_id"] = df.apply(lambda x:'%s%s%s%s' % (
                                    map_gender(x['gender']), map_age(x['age']), 
                                    map_color(x['color']), map_literacy(x['literacy'])
                                ), axis=1)
    df.to_hdf(output_file, 'table')

if __name__ == '__main__':
    main()
