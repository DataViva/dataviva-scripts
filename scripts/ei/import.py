import click
import os, sys, fnmatch
import re

pattern = re.compile('output_(\w+)_\d+_\d+.csv')
pattern2 = re.compile('output_(\w+)_(ten_percent|supersmall).csv')


def parse_table(t):
    m = pattern.search(t)
    if m:
        return "ei_" + m.group(1)
    m = pattern2.search(t)
    return "ei_" + m.group(1)

# via http://stackoverflow.com/questions/13299731/python-need-to-loop-through-directories-looking-for-txt-files
def findFiles (path, filter):
    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, filter):
            yield os.path.join(root, file)


@click.command()
@click.option('--idir', default='.', prompt=False,
              help='Directory for csv files.')
def main(idir):
   for f in findFiles(idir, '*.csv'):
        print f
        handle = open(f)

        tablename = parse_table(f)

        header = handle.readline().strip()
        fields = header.split(";")
        fields = ",".join(fields)
        cmd = '''mysql -uroot -h $DATAVIVA_DB_HOST $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES (%s);" ''' % (f, tablename, fields)
        print cmd
        os.system(cmd)

if __name__ == '__main__':
    main()
