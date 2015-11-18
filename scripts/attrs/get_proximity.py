''' Import statements '''
import os, sys, click, MySQLdb
import pandas as pd
import pandas.io.sql as sql
import numpy as np

'''
USAGE:
python scripts/attrs/get_proximity.py -y 2014 -a cnae -i bra -o data/attr -t rais_ybi -c num_emp
'''

''' Load product space calculations lib '''
file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../", "lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

depths = {
    "bra": [1, 3, 5, 7, 9],
    "hs": [2, 6],
    "wld": [2, 5],
    "cnae": [1, 3, 6],
    "cbo": [1, 4],
    "course_sc": [2, 5],
    "course_hedu": [2, 6],
    "university": [5],
}

attr_types = ['bra', 'hs', 'wld', 'cnae', 'cbo', 'course_sc', 'course_hedu', 'university']

def get_years(year):
    if "," not in year and "-" not in year:
        return [int(year)]
    if "-" in year:
        start, end = year.split("-")
        return range(int(start), int(end)+1)
    if "," in year:
        return [int(y) for y in year.split(",")]

@click.command()
@click.option('-y', '--year', prompt='Year', help='year of the data to convert', required=True)
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
# @click.option('attr', '--attr', '-a', click.Choice(attr_types), required=True, prompt=True)
@click.option('-a', '--attr', type=click.Choice(attr_types), required=True, prompt='Attr')
@click.option('-i', '--i_attr', type=click.Choice(attr_types), required=True, prompt='Intermediate Attr')
@click.option('-t', '--table', required=True, prompt='DB Table')
@click.option('-c', '--column', required=True, prompt='DB Column')
def prox(year, output_path, attr, i_attr, table, column):
    
    attr_depths = depths[attr]
    i_attr_depths = depths[i_attr]
    
    years = get_years(year)
    
    for year in years:
        print "year:", year
        
        for i, depth in enumerate(attr_depths):
            print attr, "depth:", depth
            
            query = """
                SELECT {0}_id, {1}_id, {2}
                FROM {3}
                WHERE year=%s
            """.format(attr, i_attr, column, table)
            
            if len(attr_depths) > 1:
                query += " and {}_id_len={}".format(attr, depth)
            
            if len(i_attr_depths) > 1:
                query += " and {}_id_len={}".format(i_attr, i_attr_depths[-1])
            
            if "secex" in table:
                query += " and month=0"
            
            data = sql.read_sql(query, db, params=[year])
            data = data.pivot(index="{}_id".format(i_attr), columns="{}_id".format(attr), values=column)
        
            rcas = ps_calcs.rca(data)
        
            rcas[rcas >= 1] = 1
            rcas[rcas < 1] = 0
        
            prox = ps_calcs.proximity(rcas)
            prox = pd.DataFrame(prox.unstack(), columns=["{}_proximity".format(i_attr)])
            prox["year"] = year
            prox = prox.set_index("year", append=True)
        
            output_path_w_year = os.path.abspath(os.path.join(output_path, str(year)))
            if not os.path.exists(output_path_w_year): os.makedirs(output_path_w_year)
            fp = os.path.join(output_path_w_year, "{}_{}_proximity.tsv".format(attr, i_attr))
        
            file_mode = 'a' if i else 'w'
            user_header = False if i else True
            with open(fp, file_mode) as f:
                prox.to_csv(f, header=user_header, sep="\t")

if __name__ == "__main__":
    prox()
