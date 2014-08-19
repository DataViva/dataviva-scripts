'''
run this with:
python -m scripts.edu.undergrad_plots /Users/alexandersimoes/Desktop/education/undergraduate.csv
'''

import os, sys, click
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr

def thousands(x, pos):
    """Formatter comma seperated thousands"""
    return "{:,.0f}".format(x)

@click.command()
@click.argument('input_file', type=click.File('rb'))
@click.argument('course_name_file', type=click.File('rb'), required=False)
@click.argument('munic_name_file', type=click.File('rb'), required=False)
def main(input_file, course_name_file, munic_name_file):
    undergrad = pd.read_csv(input_file, sep=",")
    dir = os.path.dirname(input_file.name)

    if course_name_file:
        course_names = pd.read_csv(course_name_file, sep="\t")
        course_names["course_id"] = course_names["course_id"].apply(lambda x: x[1:])
        course_names = course_names.set_index("course_id")
        course_names = course_names["name_en"].to_dict()

    if munic_name_file:
        munic_names = pd.read_csv(munic_name_file, sep="\t", index_col="munic_id")
        state_names = munic_names.copy()
        munic_names = munic_names["munic_name"].to_dict()
        state_names = state_names["state_name"].to_dict()
    
    years = list(undergrad['Year'].value_counts().index)
    years.sort()
    
    thousand_format = tkr.FuncFormatter(thousands)
    
    ##########################################################################
    ##
    #  Enrolled students histogram
    ##
    ##########################################################################
    '''
    colors = ['y', 'm', 'c', 'r', 'g', 'b']
    for i, y in enumerate(years[(len(years)/2):]):
        ugrad_year = undergrad[undergrad['Year'] == y]
        max_enrolled = int(ugrad_year['Enrolled'].max())
        count, division = np.histogram(ugrad_year['Enrolled'], bins=range(0, 1200, 5))
    
        # plt.hist(ugrad_year['Enrolled'].values, log=True, bins=200)
    
        ax = plt.scatter(division[:-1], count, s=20, facecolors='none', edgecolors=colors[i%len(colors)], label=y)

    plt.xlim((-20, 1200))
    plt.ylim((0, plt.ylim()[1]))

    plt.legend(loc='upper right');
    plt.show()
    '''
    
    #########################################################################
    ##
    #  Academic Organization - University (1) University Center (2) 
    #  Integrated Faculties (3) Graduate Program (4)
    ##
    #########################################################################
    '''
    df = undergrad.groupby(['Year', 'Academic_organization']).sum()['Enrolled']
    df = df.reset_index().pivot(index="Academic_organization", columns="Year", values='Enrolled')
    df = df.reset_index()
    df["Academic_organization"] = df["Academic_organization"].replace({1:"University", 2:"University Center", 3:"Integrated Faculties", 4:"Graduate Program"})
    df = df.set_index("Academic_organization")
    df = df.T
    print df
    
    ax = df.plot(kind='bar')
    ax.set_ylabel('Number of Entrolled Students')
    ax.set_xlabel('Academic Organization')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper left', prop={'size':8})
    plt.tight_layout()
    # plt.show()
    df.to_csv(os.path.join(dir, "undergrad", "academic_organization.csv"))
    plt.savefig(os.path.join(dir, "undergrad", "academic_organization.pdf"))
    '''
    
    #########################################################################
    ##
    #  Municipality
    ##
    #########################################################################
    '''
    df = undergrad.groupby(['Year', 'Municipality']).sum()['Enrolled']
    df = df.reset_index().pivot(index="Municipality", columns="Year", values='Enrolled')
    top = df.sum(axis=1).order(ascending=False).index[:20]
    df = df.reindex(top)
    df = df.reset_index()
    df["Municipality"] = df["Municipality"].replace(munic_names)
    df = df.set_index("Municipality")
    print df.head()
    
    ax = df.plot(kind='bar', fontsize=8)
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Municipality')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper right', prop={'size':8})
    plt.tight_layout()
    # plt.show()
    plt.savefig(os.path.join(dir, "undergrad", "munic.pdf"), figsize=(20, 14), dpi=100)
    df.to_csv(os.path.join(dir, "undergrad", "munic.csv"))
    '''
    
    #########################################################################
    ##
    #  States
    ##
    #########################################################################)
    '''
    df = undergrad.copy()
    df["Municipality"] = df["Municipality"].replace(state_names)
    df = df.groupby(['Year', 'Municipality']).sum()['Enrolled']
    df = df.reset_index().pivot(index="Municipality", columns="Year", values='Enrolled')
    
    ax = df.plot(kind='bar', fontsize=8)
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('State')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper left', prop={'size':8})
    plt.tight_layout()
    # plt.show()
    plt.savefig(os.path.join(dir, "undergrad", "states.pdf"), figsize=(20, 14), dpi=100)
    df.to_csv(os.path.join(dir, "undergrad", "states.csv"))
    '''
    
    #########################################################################
    ##
    #  Enrolled students per year
    ##
    #########################################################################
    '''
    df = undergrad.groupby('Year').sum()[["Openings", "Entrants", "Enrolled", "Graduates"]]
    ax = df.plot(kind='bar')
    ax.set_ylabel('Total Entrolled Students')
    ax.set_xlabel('Year')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.tight_layout()
    # plt.show()
    plt.savefig(os.path.join(dir, "undergrad", "years.pdf"), figsize=(20, 14), dpi=100)
    df.to_csv(os.path.join(dir, "undergrad", "years.csv"))
    '''
    
    # df = undergrad.groupby('Year').sum()['Enrolled']
    # ax = df.plot(kind='bar')
    # ax.set_ylabel('Total Entrolled Students')
    # ax.set_xlabel('Year')
    # ax.yaxis.set_major_formatter(thousand_format)
    # # plt.show()
    # plt.savefig(os.path.join(dir, "undergrad", "years.pdf"), figsize=(20, 14), dpi=100)
    # df.to_csv(os.path.join(dir, "undergrad", "years.csv"))    
    
    #########################################################################
    ##
    #  Admin Category - federal (1) state (2) local (3) profit (4)
    #  non-profit (5) special (6)
    ##
    #########################################################################
    '''
    df = undergrad.groupby(['Year', 'Adm_category']).sum()['Enrolled']
    df = df.reset_index().pivot(index="Adm_category", columns="Year", values='Enrolled')
    df = df.reset_index()
    df["Adm_category"] = df["Adm_category"].replace({1:"Federal", 2:"State", 3:"Local", 4:"Profit", 5:"Non-profit", 6:"Special"})
    df = df.set_index("Adm_category")
    print df
    
    ax = df.T.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Administrative Category')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper left', prop={'size':8})
    plt.tight_layout()
    # plt.show()
    plt.savefig(os.path.join(dir, "undergrad", "adm_category.pdf"), figsize=(20, 14), dpi=100)
    df.T.to_csv(os.path.join(dir, "undergrad", "adm_category.csv"))
    # plt.savefig(os.path.join(dir, "undergrad_admin_category.pdf"))
    '''
    
    #########################################################################
    ##
    #  Courses
    ##
    #########################################################################
    '''
    courses = undergrad.groupby(['Year', 'ID_course']).sum()['Enrolled']
    courses = courses.reset_index().pivot(index="ID_course", columns="Year", values='Enrolled')
    top = courses.sum(axis=1).order(ascending=False).index[:20]
    courses = courses.reindex(top)
    courses = courses.reset_index()
    courses["ID_course"] = courses["ID_course"].replace(course_names)
    courses = courses.set_index("ID_course")
    
    ax = courses.plot(kind='bar', fontsize=8)
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Course')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper right', prop={'size':8})
    plt.tight_layout()
    # plt.show()
    
    plt.savefig(os.path.join(dir, "undergrad", "courses.pdf"), figsize=(20, 14), dpi=100)
    courses.to_csv(os.path.join(dir, "undergrad", "courses.csv"))
    '''
    
    #########################################################################
    ##
    #  Modality - class (1) remote (2)
    ##
    #########################################################################
    '''
    df = undergrad.groupby(['Year', 'Modality']).sum()['Enrolled']
    df = df.reset_index().pivot(index="Modality", columns="Year", values='Enrolled')
    df = df.reset_index()
    df["Modality"] = df["Modality"].replace({1:"Class", 2:"Remote"})
    df = df.set_index("Modality")
    print df
    
    ax = df.T.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Modality')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper left', prop={'size':8})
    plt.tight_layout()
    # plt.show()
    plt.savefig(os.path.join(dir, "undergrad", "modality.pdf"), figsize=(20, 14), dpi=100)
    df.T.to_csv(os.path.join(dir, "undergrad", "modality.csv"))
    '''
    
    #########################################################################
    ##
    #  Level - Undergrad (1) Associate (2)
    ##
    #########################################################################
    
    df = undergrad.groupby(['Year', 'Level']).sum()['Enrolled']
    df = df.reset_index().pivot(index="Level", columns="Year", values='Enrolled')
    df = df.reset_index()
    df["Level"] = df["Level"].replace({1:"Undergrad", 2:"Associate"})
    df = df.set_index("Level")
    print df
    
    ax = df.T.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Level')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper left', prop={'size':8})
    plt.tight_layout()
    # plt.show()
    plt.savefig(os.path.join(dir, "undergrad", "level.pdf"), figsize=(20, 14), dpi=100)
    df.T.to_csv(os.path.join(dir, "undergrad", "level.csv"))
    
    #########################################################################
    ##
    #  Degree - Bachelor (1) Licentiate (2) Technological (3)
    ##
    #########################################################################
    '''
    df = undergrad.groupby(['Year', 'Degree']).sum()['Enrolled']
    df = df.reset_index().pivot(index="Degree", columns="Year", values='Enrolled')
    df = df.reset_index()
    df["Degree"] = df["Degree"].replace({1:"Bachelor", 2:"Licentiate", 3:"Technological"})
    df = df.set_index("Degree")
    print df
    
    ax = df.T.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Degree')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper left', prop={'size':8})
    plt.tight_layout()
    # plt.show()
    plt.savefig(os.path.join(dir, "undergrad", "degree.pdf"), figsize=(20, 14), dpi=100)
    df.T.to_csv(os.path.join(dir, "undergrad", "degree.csv"))
    '''
    

if __name__ == "__main__":
    main()