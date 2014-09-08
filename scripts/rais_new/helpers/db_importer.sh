#!/bin/bash
MODE="rais"

if [ -z "$1" ]
  then
    echo "** ERROR: No directory supplied. Please path an absolute folder path.";
    exit -1;
fi


# -- one parameter required absolute path to folder
FOLDER=$1

for fullpath in $FOLDER/*.tsv.bz2
do
    echo "Full path: $fullpath";
    filename=$(basename $fullpath)
    tsvname="${filename%.bz2}";
    
    # -- Unzip
    echo "Unzipping $fullpath ..."
    bzcat $fullpath > $FOLDER/$tsvname

    tsvpath="$FOLDER/$tsvname"

    # -- Import to database
    tablename="${MODE}_${filename%.tsv.bz2}";
    echo "Importing $tsvpath to SQL table $tablename";

    # -- Take the first line of the tsv and convert tabs to commas so we can map tsv fields -> mysql columns
    fields=`head -1 $tsvpath | tr '\t' ','`
    echo $fields;
    mysql -uroot $DATAVIVA2_DB_NAME -e "LOAD DATA LOCAL INFILE '$tsvpath' INTO TABLE $tablename FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES ($fields);";

    # -- Clean up tmp file
    rm $FOLDER/$tsvname;
    echo "Completed import to $tablename";

done
 


