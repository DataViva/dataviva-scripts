import sys, os
import pandas as pd
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../", "lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs

def calc_rca(ybuc, year):

    ybc = ybuc.groupby(level=["year", "bra_id", "course_hedu_id"]).sum()
    ybc = ybc[["enrolled"]]
    ybc = ybc.reset_index()
    ybc = ybc.drop("year", axis=1)

    rcas = ybc.pivot(index="bra_id", columns="course_hedu_id", values="enrolled")
    rcas = ps_calcs.rca(rcas)
    rcas = pd.DataFrame(rcas.stack(), columns=["enrolled_rca"])

    rcas = rcas.replace(0, np.nan)
    rcas = rcas.dropna(how="all")

    rcas["year"] = int(year)
    rcas = rcas.set_index("year", append=True)
    rcas = rcas.swaplevel("year", "course_hedu_id")
    rcas = rcas.swaplevel("year", "bra_id")

    return rcas