# db_importer.py

import click
import os, sys, fnmatch
import re

'''
USAGE:
python db_importer.py --idir=data/hedu/2012/ --name=hedu
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

@click.command()
@click.option('--idir', default='.', type=click.Path(exists=True), prompt=False, help='Directory for tsv files.')
@click.option('--name',prompt=True, help='Name of database eg rais or secex.')
def main(idir, name):
   for f in findFiles(idir, '*.tsv*'):
        bzipped = False
        # print f, "Processing"
        if f.endswith("bz2"):
            bzipped = True
            os.system("bunzip2 -k " + f)
            f = f[:-4]
        handle = open(f)
        tablename = parse_table(f, name)
        print "importing", f, "into", tablename
        header = handle.readline().strip()
        fields = header.split('\t')
        fields_null = ["{0} = nullif(@v{0},'')".format(fi) for fi in fields]
        # print "fields", fields
            
        fields = ["@v{}".format(x) for x in fields]

        fields = ",".join(fields)
        fields_null = ",".join(fields_null)

        cmd = '''mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES (%s) SET %s;" ''' % (f, tablename, fields, fields_null)
        # print cmd
        os.system(cmd)
        
        # delete bunzipped file
        if bzipped:
            os.remove(f)

if __name__ == '__main__':
    main()
