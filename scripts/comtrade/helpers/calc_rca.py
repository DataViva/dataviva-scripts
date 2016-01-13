import sys, os
import pandas as pd
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../../lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

def calc_rca(ypw):
    ypw_rca = ypw.reset_index()
    ypw_rca = ypw_rca.pivot(index="wld_id", columns="hs_id", values="val_usd")
    ypw_rca = ps_calcs.rca(ypw_rca)
    ypw_rca = pd.DataFrame(ypw_rca.T.stack(), columns=["rca"])
    ypw_rca = ypw_rca.replace(0, np.nan)
    
    # print ypw_rca.head()
    # print ypw.head()
    # sys.exit()
    
    ypw['rca'] = ypw_rca['rca']
    
    return ypw
