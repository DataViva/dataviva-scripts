import click, os, sys, MySQLdb
import pandas as pd

db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], 
                     user=os.environ["DATAVIVA_DB_USER"], 
                     passwd=os.environ["DATAVIVA_DB_PW"], 
                     db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

'''
USAGE:
python run_all_years.py -s 2000 -e 2000 -m secex_export
'''

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
def main(file_path):
    cols = (['year', 'state', 'bra_id', 'hdi', 'hdi_edu', 'hdi_life', 'hdi_income', 'life_exp', \
        'fertility', 'mort_1', 'mort_5', 'illiteracy', 'gini', 'very_poor', 'poor', 'income_pc', 'theil', \
        'self_employed', 'employers', 'p_agro', 'p_com', 'p_contr', 'p_extr', 'p_formal', 'unemployed', \
        'theiltrab', 'water', 'water_toilet', 'household', 'garbage', 'electricity', 'bad_water', \
        'econ_active_10', 'econ_active_18', 'rural', 'pop', 'urban', 'pop_10', 'pop_18'])
    ibge_munics = pd.io.excel.read_excel(file_path, sheetname=1, header=0, names=cols)
    ibge_munics = ibge_munics.drop('state', axis=1)
    
    a, b = cols.index('state'), cols.index('bra_id')
    cols[b], cols[a] = cols[a], cols[b]
    ibge_states = pd.io.excel.read_excel(file_path, sheetname=2, header=0, names=cols)
    ibge_states = ibge_states.drop('state', axis=1)
    
    cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 9;")
    munic_lookup = {int(r[0]):r[1] for r in cursor.fetchall()}
    ibge_munics = ibge_munics.replace({"bra_id": munic_lookup})
    ibge_munics = ibge_munics.set_index(["year", "bra_id"])
    
    cursor.execute("select id_ibge, id from attrs_bra where id_ibge is not null and length(id) = 3;")
    state_lookup = {int(r[0]):r[1] for r in cursor.fetchall()}
    ibge_states = ibge_states.replace({"bra_id": state_lookup})
    ibge_states = ibge_states.set_index(["year", "bra_id"])
    
    for (year, bra_id), munic in ibge_munics.iterrows():
        for stat_id, stat_val in munic.iteritems():
            if not pd.isnull(stat_val):
                # print year, bra_id, stat_id, stat_val
                cursor.execute("insert into attrs_ybs values(%s, %s, %s, %s)", [year, bra_id, stat_id, stat_val])
    
    for (year, bra_id), state in ibge_states.iterrows():
        for stat_id, stat_val in state.iteritems():
            if not pd.isnull(stat_val):
                # print year, bra_id, stat_id, stat_val
                cursor.execute("insert into attrs_ybs values(%s, %s, %s, %s)", [year, bra_id, stat_id, stat_val])
        
        

if __name__ == '__main__':
    main()
