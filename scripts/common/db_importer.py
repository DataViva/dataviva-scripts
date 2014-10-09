# db_importer.py

import click
import os, sys, fnmatch
import re

'''
USAGE:
python db_importer.py --idir=/Users/jspeiser/output/hedu/2012/ --name=hedu
'''

pattern = re.compile('(\w+).tsv(.bz2)*')


def parse_table(t, dbname):
    m = pattern.search(t)
    if m:
        return dbname + "_" + m.group(1)

# via http://stackoverflow.com/questions/13299731/python-need-to-loop-through-directories-looking-for-txt-files
def findFiles (path, filter):
    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, filter):
            yield os.path.join(root, file)


special_map = {"sc" : {"grade": "grade_id", "ethnicity": "ethnicity_id"}}

@click.command()
@click.option('--idir', default='.', type=click.Path(exists=True), prompt=False, help='Directory for tsv files.')
@click.option('--name',prompt=True, help='Name of database eg rais or secex.')
def main(idir, name):
   for f in findFiles(idir, '*.tsv*'):
        print f, "Processing"
        if f.endswith("bz2"):
            os.system("bunzip2 -k " + f)
            f = f[:-4]
        print f
        handle = open(f)
        tablename = parse_table(f, name)
        print "table name =", tablename
        header = handle.readline().strip()
        fields = header.split('\t')
        print "fields", fields
            
        fields = [x for x in fields]

        if name in special_map:
            mymap = special_map[name]
            fields = [ mymap[x] if x in mymap else x for x in fields ]

        fields = ",".join(fields)
        print
        print

        cmd = '''mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES (%s);" ''' % (f, tablename, fields)
        print cmd
        os.system(cmd)

if __name__ == '__main__':
    main()
