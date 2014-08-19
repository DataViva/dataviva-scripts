# -*- coding: utf-8 -*-
"""
    Exploration
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

''' Import statements '''
import os, sys, click
import pandas as pd

@click.command()
@click.argument('input_file', type=click.File('rb'))
def main(input_file):
    undergrad = pd.read_csv(input_file, sep=",")
    
    # find dupliates
    undergrad['is_duplicated'] = undergrad.duplicated(['Year', 'Municipality', 'ID_university', 'ID_course'])
    
    print undergrad[undergrad['is_duplicated']==False].shape
    print undergrad[undergrad['is_duplicated']==True].shape
    print undergrad[undergrad['is_duplicated']==True].head()
    
    # print undergrad[(undergrad['Year']==2000) & \
    #                     (undergrad['Municipality']==3205309) & \
    #                     (undergrad['ID_university']==6) & \
    #                     (undergrad['ID_course']=='345A01')].head()
    
if __name__ == "__main__":
    main()