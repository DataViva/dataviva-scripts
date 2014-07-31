'''
run this with:
python -m scripts.edu.school_plots /Users/alexandersimoes/Desktop/education/School_census.csv /Users/alexandersimoes/Downloads/school_census_course_names.tsv /Users/alexandersimoes/Desktop/education/munic_names.tsv
'''

import os,sys,click
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
    dir = os.path.dirname(input_file.name)
    
    tech = pd.read_csv(input_file, sep=",")
    
    years = list(tech['YEAR'].value_counts().index)
    
    if course_name_file:
        course_names = pd.read_csv(course_name_file, sep="\t", index_col="id")
        course_names = course_names["name_en"].to_dict()
    
    if munic_name_file:
        munic_names = pd.read_csv(munic_name_file, sep="\t", index_col="munic_id")
        state_names = munic_names.copy()
    
        munic_names = munic_names["munic_name"].to_dict()
        state_names = state_names["state_name"].to_dict()

    thousand_format = tkr.FuncFormatter(thousands)
    
    ##########################################################################
    ##
    #  Entries per year
    ##
    ##########################################################################
    '''
    ax = tech['YEAR'].value_counts().order(ascending=True).plot(kind='bar')
    ax.set_ylabel('Number of Entries')
    ax.set_xlabel('Year')
    plt.show()
    '''

    #########################################################################
    ##
    #  Enrolled students per year
    ##
    #########################################################################
    '''
    enrolled_by_year = tech.groupby('YEAR').sum()['QTY_ENROLL']
    print enrolled_by_year
    ax = enrolled_by_year.plot(kind='bar')
    ax.set_ylabel('Total Enrolled Students')
    ax.set_xlabel('Year')
    # plt.show()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "school_census", "enrolled_per_year.pdf"))
    '''
    
    ##########################################################################
    ##
    #  Enrolled students histogram
    ##
    ##########################################################################
    '''
    colors = ['y', 'm', 'c', 'r', 'g', 'b']
    fig, ax = plt.subplots(figsize=(9, 7))
    for i, y in enumerate(years):
        enrolled = tech[tech['YEAR'] == y]
        count, division = np.histogram(enrolled['QTY_ENROLL'], bins=range(101))
        ax.scatter(division[:-1], count, s=20, facecolors='none', edgecolors=colors[i%len(colors)], label=y)
        ax.set_ylabel('Frequency')
        ax.set_xlabel('Number of Enrolled Students')
        ax.grid(True, linestyle='--', which='major', color='grey', alpha=0.75)

    plt.xlim((0, 100))
    plt.ylim((0, plt.ylim()[1]))
    
    x = range(0, 101, 5)[1:]
    x.insert(0, 1)
    plt.xticks(x)

    plt.legend(loc='upper right');
    # plt.show()
    plt.savefig(os.path.join(dir, "school_census", "enrolled_distribution.pdf"))
    '''

    #########################################################################
    ##
    #  Education Level
    ##
    #########################################################################
    '''
    edu_level = pd.DataFrame()
    for y in years:
        if edu_level.empty:
            edu_level = pd.DataFrame(tech[tech['YEAR']==y]['ID_EDUCATION_LEVEL'].value_counts(), columns=[y])
        else:
            edu_level[y] = tech[tech['YEAR']==y]['ID_EDUCATION_LEVEL'].value_counts()
    edu_level = edu_level.T
    edu_level.columns = edu_level.columns[::-1]
    edu_level = edu_level.rename(columns={1:"Integrated", 2:"Professional"})
    print edu_level
    ax = edu_level.plot(kind='bar')
    ax.set_ylabel('Number of Entries')
    ax.set_xlabel('Education Level')
    # plt.show()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "school_census", "education_level.pdf"))
    '''
    
    #########################################################################
    ##
    #  Courses
    ##
    #########################################################################
    '''
    courses = tech.groupby(['YEAR', 'ID_COURSE']).sum()['QTY_ENROLL']
    courses = courses.reset_index().pivot(index="ID_COURSE", columns="YEAR", values='QTY_ENROLL')
    top = courses.sum(axis=1).order(ascending=False).index[:20]
    courses = courses.reindex(top)
    courses = courses.reset_index()
    courses["ID_COURSE"] = courses["ID_COURSE"].replace(course_names)
    courses = courses.set_index("ID_COURSE")
    print courses
    
    ax = courses.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Course')
    plt.legend(loc='upper right')
    plt.tight_layout()
    # plt.show()
    courses.to_csv(os.path.join(dir, "school_census", "top_courses.csv"))
    plt.savefig(os.path.join(dir, "school_census", "top_courses.pdf"))
    '''
    
    #########################################################################
    ##
    #  Municipality
    ##
    #########################################################################
    '''
    munics = tech.groupby(['YEAR', 'ID_MUNICIPALITY']).sum()['QTY_ENROLL']
    munics = munics.reset_index().pivot(index="ID_MUNICIPALITY", columns="YEAR", values='QTY_ENROLL')
    top = munics.sum(axis=1).order(ascending=False).index[:20]
    munics = munics.reindex(top)
    munics = munics.reset_index()
    munics["ID_MUNICIPALITY"] = munics["ID_MUNICIPALITY"].replace(munic_names)
    munics = munics.set_index("ID_MUNICIPALITY")
    print munics.head()
    
    ax = munics.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Municipality')
    plt.legend(loc='upper right')
    plt.tight_layout()
    # plt.show()
    munics.to_csv(os.path.join(dir, "school_census", "munics.csv"))
    plt.savefig(os.path.join(dir, "school_census", "munics.pdf"))
    '''
    
    #########################################################################
    ##
    #  States
    ##
    #########################################################################
    '''
    states = tech.copy()
    states["ID_MUNICIPALITY"] = states["ID_MUNICIPALITY"].replace(state_names)
    states = states.groupby(['YEAR', 'ID_MUNICIPALITY']).sum()['QTY_ENROLL']
    states = states.reset_index().pivot(index="ID_MUNICIPALITY", columns="YEAR", values='QTY_ENROLL')
    print states
    
    ax = states.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('States')
    plt.legend(loc='upper left')
    plt.tight_layout()
    # plt.show()
    plt.savefig(os.path.join(dir, "school_census", "states.pdf"))
    '''
    
    #########################################################################
    ##
    #  Location - Urban (1) vs Rural (2)
    ##
    #########################################################################
    '''
    loc = tech.groupby(['YEAR', 'ID_LOCATION']).sum()['QTY_ENROLL']
    loc = loc.reset_index().pivot(index="ID_LOCATION", columns="YEAR", values='QTY_ENROLL')
    loc = loc.reset_index()
    loc["ID_LOCATION"] = loc["ID_LOCATION"].replace({1:"Urban", 2:"Rural"})
    loc = loc.set_index("ID_LOCATION")
    print loc
    
    ax = loc.T.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Location - Urban (1) vs Rural (2)')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper left')
    plt.tight_layout()
    # plt.show()
    loc.T.to_csv(os.path.join(dir, "school_census", "location.csv"))
    plt.savefig(os.path.join(dir, "school_census", "location.pdf"))
    '''
    
    #########################################################################
    ##
    #  Funding - Federal (1) State (2) Local (3) Private (4)
    ##
    #########################################################################
    '''
    loc = tech.groupby(['YEAR', 'ID_ADM_DEPENDENCY']).sum()['QTY_ENROLL']
    loc = loc.reset_index().pivot(index="ID_ADM_DEPENDENCY", columns="YEAR", values='QTY_ENROLL')
    loc = loc.reset_index()
    loc["ID_ADM_DEPENDENCY"] = loc["ID_ADM_DEPENDENCY"].replace({1:"Federal", 2:"State", 3:"Local", 4:"Private"})
    loc = loc.set_index("ID_ADM_DEPENDENCY")
    print loc
    
    ax = loc.T.plot(kind='bar')
    ax.set_ylabel('Number of Enrolled Students')
    ax.set_xlabel('Funding - Federal (1) State (2) Local (3) Private (4)')
    ax.yaxis.set_major_formatter(thousand_format)
    plt.legend(loc='upper left')
    plt.tight_layout()
    # plt.show()
    loc.T.to_csv(os.path.join(dir, "school_census", "funding.csv"))
    plt.savefig(os.path.join(dir, "school_census", "funding.pdf"))
    '''
    

if __name__ == "__main__":
    main()