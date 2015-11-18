# -*- coding: utf-8 -*-

import click
import os, sys, fnmatch
import re

'''
USAGE:
python import.py --idir=/Users/jspeiser/output/sc/2007/
'''

pattern = re.compile('(\w+).tsv(.bz2)*')
pattern1 = re.compile('(\w+)_(gender|color|loc|school_type|cid2).tsv(.bz2)*')

def parse_table(t):
    t = t.replace('_with_growth', '')
    m = pattern1.search(t)
    if m:
        return "sc_" + m.group(1)
    m = pattern.search(t)
    if m:
        return "sc_" + m.group(1)

# via http://stackoverflow.com/questions/13299731/python-need-to-loop-through-directories-looking-for-txt-files
def findFiles (path, filter):
    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, filter):
            yield os.path.join(root, file)

import codecs

@click.command()
@click.option('--idir', default='.', prompt=False,
              help='Directory for tsv files.')
def main(idir):
    for f in findFiles(idir, '*.tsv*'):
        bzipped = False
        print "Processing", f
        if f.endswith("bz2"):
            bzipped = True
            os.system("bunzip2 -k " + f)
            f = f[:-4]
        # print f
        handle = codecs.open(f, encoding='utf-8-sig')
        tablename = parse_table(f)
        # print "table name =", tablename
        header = handle.readline().strip()
        fields = header.split('\t')
        fields = [x for x in fields if x!='schools']
        if 'class_id' in fields: fields[fields.index('class_id')] = 'classes'
        if 'enroll_id' in fields: fields[fields.index('enroll_id')] = 'enrolled'
        if 'enroll_id_growth' in fields: fields[fields.index('enroll_id_growth')] = 'enrolled_growth'
        if 'enroll_id_growth_5' in fields: fields[fields.index('enroll_id_growth_5')] = 'enrolled_growth_5'
        
	table_suffix = tablename.split("sc_")[-1]
        if 'school_id' in fields and "s" not in table_suffix: fields[fields.index('school_id')] = 'num_schools'

        fields = ",".join(fields).decode('utf-8')
	print fields, tablename

        cmd = '''mysql -h $DATAVIVA_DB_HOST -u$DATAVIVA_DB_USER -p$DATAVIVA_DB_PW $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES ({}) ;" '''.format(f, tablename, fields)

	print
        print cmd
	print

        os.system(cmd)
        
        # delete bunzipped file
        if bzipped:
            os.remove(f)


if __name__ == '__main__':
    main()
