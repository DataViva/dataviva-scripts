# db_importer.py

import click
import os
import fnmatch
import re

'''
USAGE:
python db_importer.py --idir=data/hedu/2012/ --name=hedu
'''

pattern = re.compile('(\w+).csv(.bz2)*')


def parse_table(t, dbname):
    m = pattern.search(t)
    if m:
        return dbname + "_" + m.group(1)

# via http://stackoverflow.com/questions/13299731/python-need-to-loop-through-directories-looking-for-txt-files


def findFiles(path, filter):
    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, filter):
            yield os.path.join(root, file)


@click.command()
@click.option('--idir', '-i', default='.', type=click.Path(exists=True), prompt=False, help='Directory for csv files.')
@click.option('--name', '-n', prompt=True, help='Name of database eg rais or secex.')
@click.option('--host', '-h', prompt=True, help='Database host ip.')
@click.option('--user', '-u', prompt=True, help='Database user.')
@click.option('--password', '-p', prompt=True, help='Database password.')
@click.option('--database', '-d', prompt=True, help='Database name.')
def main(idir, name, host, user, password, database):
    for f in findFiles(idir, '*.csv*'):
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
        fields = header.split(',')
        fields_null = ["{0} = nullif(@v{0},'')".format(fi) for fi in fields]
        # print "fields", fields

        fields = ["@v{}".format(x) for x in fields]

        fields = ",".join(fields)
        fields_null = ",".join(fields_null)

        cmd = '''mysql -h %s -u%s -p%s %s --local-infile=1 -e "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s
                 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (%s) SET %s;" ''' % (
            host, user, password, database, f, tablename, fields, fields_null)
        # print cmd
        os.system(cmd)

        # delete bunzipped file
        if bzipped:
            os.remove(f)

if __name__ == '__main__':
    main()
