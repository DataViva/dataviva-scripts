#!/bin/bash
MODE="rais"

if [ -z "$1" ]
  then
    echo "** ERROR: No directory supplied. Please specify absolute path to folder.";
    exit -1;
fi
FOLDER=$1

if [ -z "$2" ]
  then
    echo "** ERROR: please specify tables.";
    exit -1;
fi
TABLES=$2
IFS='_'


# for fullpath in $FOLDER/*.tsv.bz2
# for tbl in yb ybi ybio ybo yi yio yo
for tbl in $TABLES
do
    # echo "Full path: $fullpath";
    # filename=$(basename $fullpath)
    # tsvname="${filename%.bz2}";
    filename="$FOLDER/$tbl.tsv.bz2";
    tsvname="${filename%.bz2}";
    echo $tsvname

    # -- Unzip
    echo "Unzipping to $tsvname ..."
    bzcat $filename > $tsvname

    # tsvpath="$FOLDER/$tsvname"

    # -- Import to database
    tablename="${MODE}_$tbl";
    echo "Importing $tsvname to SQL table $tablename";

    # -- Take the first line of the tsv and convert tabs to commas so we can map tsv fields -> mysql columns
    fields=`head -1 $tsvname | tr '\t' ','`
    echo $fields;
    mysql -uroot $DATAVIVA2_DB_NAME -e "LOAD DATA LOCAL INFILE '$tsvname' INTO TABLE $tablename FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES ($fields);";

    # -- Clean up tmp file
    rm $tsvname;
    echo "Completed import to $tablename";
done



