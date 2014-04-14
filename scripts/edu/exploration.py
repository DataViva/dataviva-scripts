# -*- coding: utf-8 -*-
"""
    Exploration
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

''' Import statements '''
import csv, sys, os, argparse, MySQLdb, time
from os import environ
from scripts import YEAR
import pandas as pd

def main():
    tech = pd.read_csv('/Users/alexandersimoes/Desktop/education/School_census.csv', sep=";")
    
    # check types
    # print tech.dtypes
    
    # replace commas to periods
    tech["QTY_ENROLL"] = tech["QTY_ENROLL"].apply(lambda x: str(x).replace(",", "."))

    # set type of column
    tech["QTY_ENROLL"] = tech["QTY_ENROLL"].astype(float)
    
    # find dupliates
    # tech['is_duplicated'] = tech.duplicated(['YEAR', 'ID_MUNICIPALITY', 'ID_SCHOOL', 'ID_COURSE', 'ID_CLASS'])
    
    # print tech[tech['is_duplicated']==False].shape
    # print tech[tech['is_duplicated']==True].shape
    # print tech[tech['is_duplicated']==True].head()
    
    # collapse a column
    tech = tech[["ID_CLASS", "ID_COURSE", "ID_MUNICIPALITY", "ID_SCHOOL", "QTY_ENROLL", "YEAR"]]
    
    tech = tech.set_index(['YEAR', 'ID_MUNICIPALITY', 'ID_SCHOOL', 'ID_COURSE', 'ID_CLASS'])
    tech_bra = tech.groupby(level=['YEAR', 'ID_MUNICIPALITY']).sum()
    
    print tech_bra.ix[2012]["QTY_ENROLL"].order()

if __name__ == "__main__":
    main()