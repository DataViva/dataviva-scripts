# -*- coding: utf-8 -*-
"""
    Check all attrs used in SECEX
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    Usage:
    python -m scripts.secex.step_0_check /path/to/data/secex/MDICxIBGE.csv

"""


''' Import statements '''
import csv,sys,os,MySQLdb,time,click
from ..config import DATA_DIR
from ..helpers import get_file

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

@click.command()
@click.argument('input_file', type=click.Path(exists=True))
def updateMDICxIBGE(input_file):
    '''Open CSV file'''
    click.echo(click.format_filename(input_file))
    input_file = get_file(input_file)
    delim = ";"
    csv_reader = csv.reader(input_file, delimiter=delim)
    
    for i, line in enumerate(csv_reader):
        #7 = IBGE , 9 = MDIC
        ibge_cod=line[7].strip()
        mdic_cod=line[9].strip()
        if i==0:
            continue

        sql="select id_mdic, id from attrs_bra where id_ibge = {0}".format(ibge_cod)
        cursor.execute(sql)
        values=cursor.fetchall()
        size=len(values)
        if size==1:          
            old_mdic=str(values[0][0])
            old_idbra=str(values[0][1])
            if old_mdic<>mdic_cod:
                print "Changing MDIC code from {0} to {1} in IBGE cod {2}".format(old_mdic,mdic_cod,ibge_cod)
                sql="select * from secex_yb where bra_id='{0}'".format(old_idbra)
                cursor.execute(sql)
                if len(cursor.fetchall())>0:
                    print "Exports found for {0}".format(ibge_cod)
                sql="update attrs_bra set id_mdic = {0} where id_ibge = {1}".format(mdic_cod,ibge_cod)
                cursor.execute(sql)
        else:
            print "Error finding a IBGE {0}: Found {1}".format(ibge_cod,size)



if __name__ == "__main__":
    start = time.time()
    
    updateMDICxIBGE()
    
    total_run_time = time.time() - start
    print; print;
    print "Total runtime: " + format_runtime(total_run_time)
    print; print;