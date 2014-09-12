'''
for fullpath in $FOLDER/output_yms_*.csv
do
    tablename="ei_yms"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, bra_id_s, cnae_id_s,\
            product_value, tax, icms_tax, transportation_cost, bra_id_s_len, cnae_id_s_len";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";

done
'''

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
        cmd = '''mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES (%s);" ''' % (f, tablename, fields)
        print cmd
        os.system(cmd)

if __name__ == '__main__':
    main()
