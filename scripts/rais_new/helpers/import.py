import click
import os, sys, fnmatch
import re

'''
USAGE:
python import.py --idir=/Users/jspeiser/output/rais/2002/
'''

pattern = re.compile('(\w+).tsv(.bz2)*')


def parse_table(t):
    m = pattern.search(t)
    if m:
        return "rais_" + m.group(1)

# via http://stackoverflow.com/questions/13299731/python-need-to-loop-through-directories-looking-for-txt-files
def findFiles (path, filter):
    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, filter):
            yield os.path.join(root, file)

@click.command()
@click.option('--idir', default='.', prompt=False,
              help='Directory for tsv files.')
def main(idir):
   for f in findFiles(idir, '*.tsv*'):
        print f, "Processing"
        if f.endswith("bz2"):
            os.system("bunzip2 -k " + f)
            f = f[:-4]
        print f
        handle = open(f)
        tablename = parse_table(f)
        print "table name =", tablename
        header = handle.readline().strip()
        fields = header.split('\t')
        print "fields", fields
        fields = [x for x in fields if x!='schools']

        fields = ",".join(fields)
        print
        print

        cmd = '''mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES (%s);" ''' % (f, tablename, fields)
        print cmd
        os.system(cmd)

if __name__ == '__main__':
    main()
