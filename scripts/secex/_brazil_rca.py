import MySQLdb, os, sys
import pandas.io.sql as sql

def brazil_rca(yp, year):
    
    ''' Connect to DB '''
    db = MySQLdb.connect(host=os.environ["DATAVIVA_DB_HOST"], user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    '''Get world values from database'''
    q = "select year, hs_id, rca from comtrade_ypw where year = {0} and "\
        "wld_id = 'sabra'".format(year)
    bra_rcas = sql.read_sql(q, db)
    bra_rcas = bra_rcas.set_index(["year", "hs_id"])
    
    yp["rca_wld"] = bra_rcas["rca"]
    
    return yp