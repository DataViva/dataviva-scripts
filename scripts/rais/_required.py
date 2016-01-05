# -*- coding: utf-8 -*-
"""
    Caculate required numbers YBIO
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The script is the third step in adding a new year of RAIS data to the 
    database. The script will output 1 bzipped TSV files that can then be 
    used throughout the rest of the steps.
    
    The year will needs to be specified by the user, the script will then
    loop through each geographic location to calculation the "required"
    number of employees for this YEAR-LOCATION-INDUSTRY-OCCUPATION combo.
"""

''' Import statements '''
import csv, sys, os, math, time, bz2, MySQLdb
import pandas as pd
import numpy as np
from collections import defaultdict
# from ..helpers import get_file, format_runtime
# from ..growth_lib import growth

''' Connect to DB '''
db = MySQLdb.connect(host=os.environ.get("DATAVIVA_DB_HOST", "localhost"), 
                     user=os.environ["DATAVIVA_DB_USER"], 
                     passwd=os.environ["DATAVIVA_DB_PW"], 
                     db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

def required(ybio, ybi, yi, year, depths, output_path, output_bras=False):
    
    # print "reset index", ybio.index.is_unique
    ybio = ybio.reset_index()
    ybio_data = ybio[["bra_id","cnae_id","cbo_id","num_emp"]]
    cnae_criterion = ybio_data['cnae_id'].str.len() == depths["cnae"][-1]
    cbo_criterion = ybio_data['cbo_id'].str.len() == depths["cbo"][-1]
    ybio_data = ybio_data[cnae_criterion & cbo_criterion]
    
    # ybi['required_bras'] = None
    
    ybi_calc = ybi.copy()
    ybi_calc = ybi_calc.reset_index()
    ybi_calc = ybi_calc[ybi_calc['cnae_id'].str.len() == depths["cnae"][-1]]
    ybi_calc["num_emp_est"] = ybi_calc["num_emp"] / ybi_calc["num_est"]
    ybi_calc = ybi_calc[["bra_id", "cnae_id", "num_emp_est"]]
    
    yi = yi.reset_index()
    yi = yi[yi['cnae_id'].str.len() == depths["cnae"][-1]]
    yi["num_emp_est"] = yi["num_emp"] / yi["num_est"]
    yi = yi[["cnae_id", "num_emp_est"]]
    yi = yi.set_index("cnae_id")["num_emp_est"]
    
    ybio_required = []
    ybi_required = []
    # required_bras = defaultdict(lambda: defaultdict(dict))
    # required_bras = defaultdict(list)
    for geo_level in depths["bra"]:
        bra_criterion = ybio_data['bra_id'].str.len() == geo_level
        ybio_panel = ybio_data[bra_criterion]
        ybio_panel = ybio_panel.pivot_table(index=["bra_id", "cbo_id"], \
                                            columns="cnae_id", \
                                            values="num_emp")
        ybio_panel = ybio_panel.to_panel()
        
        bra_criterion = ybi_calc['bra_id'].str.len() == geo_level
        ybi_ras = ybi_calc[bra_criterion]
        ybi_ras = ybi_ras.pivot(index="bra_id", columns="cnae_id", values="num_emp_est")
        
        rea = ybi_ras / yi
        
        bras = ybi_ras.index
        for bra in bras:
            sys.stdout.write('\r current location: ' + bra + ' ' * 10)
            sys.stdout.flush() # important
            
            rea_diff = rea - rea.ix[bra]
            rea_diff_abs = rea_diff.abs()
            rea_std = rea.std()
            
            # rea_diff_std = (rea_diff / rea_std).abs()
            
            cnaes = ybi_ras.columns
            for cnae in cnaes:
                half_std = rea_std/2
                
                similar_locs = rea_diff_abs[cnae][rea_diff_abs[cnae] <= half_std[cnae]]
                
                # max only use top 20% of all locations
                max_cutoff = int(len(bras)*.2)
                max_cutoff = max_cutoff if max_cutoff > 50 else 50
                similar_locs = similar_locs.order().ix[1:max_cutoff].index
                
                if not len(similar_locs):
                    continue
                
                req_bras_fp = os.path.abspath(os.path.join(output_path, "req_bras_test.tsv"))
                with open(req_bras_fp, "a") as myfile:
                    csvfile = csv.writer(myfile, delimiter='\t')
                    csvfile.writerow([year, bra, cnae, ",".join(similar_locs[:10])])
                
                # ybi.loc[(year, bra, cnae), 'required_bras'] = '[' + ','.join(['"{}"'.format(r) for r in ras_similar]) + ']'
                
                # ybi_required.append([year, bra, cnae, '[' + ','.join(['{}'.format(r) for r in ras_similar]) + ']'])
                # cursor.execute("INSERT INTO rais_ybi_required VALUES(%s, %s, %s, %s);", (year, bra, cnae, '[' + ','.join(['{}'.format(r) for r in ras_similar]) + ']'))
                # print [year, bra, cnae, '[' + ','.join(['"{}"'.format(r) for r in ras_similar]) + ']']
                # sys.exit()
                
                """ temporarily commented out
                required_cbos = ybio_panel[cnae].ix[list(ras_similar)].fillna(0).mean(axis=0)
                required_cbos = required_cbos[required_cbos >= 1]
            
                for cbo in required_cbos.index:
                    ybio_required.append([year, bra, cnae, cbo, required_cbos[cbo]])
                """

            if output_bras:
                cursor.executemany("update rais_ybi set required_bras = %s where year=%s and bra_id=%s and cnae_id=%s;", required_bras)
    
    
    
    # sys.exit('FINISHED required bras output!')
    
    if output_bras:
        return required_bras
    else:
        """ temporarily commented out
        print "merging ybio..."
        ybio_required = pd.DataFrame(ybio_required, columns=["year", "bra_id", "cnae_id", "cbo_id", "required"])
        ybio_required['year'] = ybio_required['year'].astype(int)
        # ybio_required['required'][ybio_required['required'] == 0] = np.nan
        ybio['year'] = ybio['year'].astype(int)
        ybio = pd.merge(ybio, ybio_required, on=["year", "bra_id", "cnae_id", "cbo_id"], how="outer")#.fillna(0)
        ybio = ybio.set_index(["year", "bra_id", "cnae_id", "cbo_id"])
        """
        
        
        """ temporarily commented out
        print "merging ybi..."
        ybi_required = pd.DataFrame(ybi_required, columns=["year", "bra_id", "cnae_id", "required"])
        ybi_required['year'] = ybi_required['year'].astype(int)
        # ybio_required['required'][ybio_required['required'] == 0] = np.nan
        ybi = ybi.reset_index()
        ybi['year'] = ybi['year'].astype(int)
        ybi = pd.merge(ybi, ybi_required, on=["year", "bra_id", "cnae_id"], how="outer")#.fillna(0)
        ybi = ybi.set_index(["year", "bra_id", "cnae_id"])
        """
    
        # print ybio.head()
        # print ybio.xs([2002, '1ac', 'a01113', '3117'])
        # sys.exit()
    
        return [ybi, ybio]
