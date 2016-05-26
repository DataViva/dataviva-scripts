import sys
import os
import pandas as pd
import numpy as np

file_path = os.path.dirname(os.path.realpath(__file__))
ps_calcs_lib_path = os.path.abspath(os.path.join(file_path, "../../", "lib/ps_calcs"))
sys.path.insert(0, ps_calcs_lib_path)
import ps_calcs


def calc_rca(ybc, year):

    rcas = pd.DataFrame()
    for geo_level in [2, 4, 8]:

        print "geo level:", geo_level

        ybc_data = ybc.reset_index()

        bra_criterion = ybc_data["bra_id"].map(lambda x: len(x) == geo_level)
        course_criterion = ybc_data["course_sc_id"].map(lambda x: len(x) == 5)
        ybc_data = ybc_data[bra_criterion & course_criterion]

        ybc_data = ybc_data[["bra_id", "course_sc_id", "students"]]

        ybc_data = ybc_data.pivot(index="bra_id", columns="course_sc_id", values="students")
        ybc_data_rca = ps_calcs.rca(ybc_data)
        ybc_data_rca = pd.DataFrame(ybc_data_rca.stack(), columns=["students_rca"])

        if rcas.empty:
            rcas = ybc_data_rca
        else:
            rcas = pd.concat([rcas, ybc_data_rca])
        rcas = rcas.replace(0, np.nan)
        rcas = rcas.dropna(how="all")

    rcas["year"] = int(year)
    rcas = rcas.set_index("year", append=True)
    rcas = rcas.swaplevel("year", "course_sc_id")
    rcas = rcas.swaplevel("year", "bra_id")
    ybc = ybc.merge(rcas, how="outer", left_index=True, right_index=True)

    return ybc
