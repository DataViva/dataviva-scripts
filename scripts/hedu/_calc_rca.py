import sys, os
import pandas as pd
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
growth_lib_path = os.path.abspath(os.path.join(file_path, "..", "growth_lib"))
sys.path.insert(0, growth_lib_path)
import growth

def calc_rca(ybuc, year):
    
    ybc = ybuc.groupby(level=["year", "bra_id", "course_id"]).sum()
    ybc = ybc[["enrolled"]]
    ybc = ybc.reset_index()
    ybc = ybc.drop("year", axis=1)
            
    rcas = ybc.pivot(index="bra_id", columns="course_id", values="enrolled")
    rcas = growth.rca(rcas)
    rcas = pd.DataFrame(rcas.stack(), columns=["enrolled_rca"])
    
    rcas = rcas.replace(0, np.nan)
    rcas = rcas.dropna(how="all")
        
    rcas["year"] = int(year)
    rcas = rcas.set_index("year", append=True)
    rcas = rcas.swaplevel("year", "course_id")
    rcas = rcas.swaplevel("year", "bra_id")
    
    return rcas