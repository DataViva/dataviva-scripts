# -*- coding: utf-8 -*-
"""
    Test RAIS data for establishment and employee ID uniqueness
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Columns:
    0 Municipality_ID
    1 EconmicAtivity_ID_ISIC
    2 EconomicActivity_ID_CNAE
    3 BrazilianOcupation_ID
    4 AverageMonthlyWage
    5 WageReceived
    6 Employee_ID
    7 Establishment_ID
    8 Year
    
    Example Usage
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

''' Import statements '''
import sys, click, csv
from scripts.helpers import get_file
from collections import defaultdict
import matplotlib.pyplot as plt
from numpy.random import normal

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
def main(file_path):
        
    raw_file = get_file(file_path)
    csv_reader = csv.reader(raw_file, delimiter=",", quotechar='"')
    header = [s.replace('\xef\xbb\xbf', '') for s in csv_reader.next()]
    
    emps = defaultdict(list)
    ests = defaultdict(list)
    
    '''Populate the data dictionaries'''
    for i, line in enumerate(csv_reader):
        
        if i % 100000 == 0 and i != 0:
            sys.stdout.write('\r lines read: ' + '{:,}'.format(i) + ' ' * 20)
            sys.stdout.flush() # important
        
        emps[line[6]].append(line[0])
        ests[line[7]].append(line[0])
    
    nonunique_emps = dict((k, v) for k, v in emps.iteritems() if len(v) > 1)
    nonunique_emps_diff_munics = dict((k, v) for k, v in nonunique_emps.iteritems() if len(set(v)) > 1)
    print; print
    print "employees:", len(emps)
    print "non-unique:", len(nonunique_emps)
    print "non-unique:", (len(nonunique_emps)/float(len(emps)))*100, "%"
    print "non-unique in diff munics:", len(nonunique_emps_diff_munics)
    print "non-unique in diff munics:", (len(nonunique_emps_diff_munics)/float(len(emps)))*100, "%"

    nonunique_ests = dict((k, v) for k, v in ests.iteritems() if len(v) > 1)
    nonunique_ests_diff_munics = dict((k, v) for k, v in nonunique_ests.iteritems() if len(set(v)) > 1)
    print
    print "establishments:", len(ests)
    print "non-unique:", len(nonunique_ests)
    print "non-unique:", (len(nonunique_ests)/float(len(ests)))*100, "%"
    print "non-unique in diff munics:", len(nonunique_ests_diff_munics)
    print "non-unique in diff munics:", (len(nonunique_ests_diff_munics)/float(len(ests)))*100, "%"
    
    # plot
    num_munics_per_emp = [len(k) for k in nonunique_emps.values()]
    plt.hist(num_munics_per_emp)
    plt.title("Number of Municipalities per Employee ID Histogram")
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    # plt.yscale('log')
    plt.show()

if __name__ == "__main__":
    main()
    