#!/bin/bash
set -x
if [ -z "$1" ]
  then
    echo "** ERROR: No directory supplied. Please path an absolute folder path.";
    exit -1;
fi

# -- one parameter required absolute path to folder
FOLDER=$1

if [ -z "$2" ]
  then
    echo "** ERROR: No trade_flow supplied. Please use either export or import as second argument.";
    exit -1;
fi

# -- one parameter required absolute path to folder
MODE="secex_$2"

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
    # fields=`head -1 $tsvpath | tr '\t' ','`
    # fields='nullif(@v'
    # fields+=`head -1 $tsvpath | sed 's/  /,""),nullif(@v/g'`
    # fields='nullif(@v$fields,"")'
    # fields+=',"")'
    sql_fields=""
    sql_set=""
    fields=`head -1 $tsvpath | tr '\t' ' '`
    for field in ${fields[@]}
    do
      sql_fields+="@v$field, "
      sql_set+="$field = nullif(@v$field,''), "
    done
    sql_set=${sql_set%", "}
    sql_fields=${sql_fields%", "}
    # echo $sql_fields
    # echo $sql_set
    # echo $fields;
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$tsvpath' INTO TABLE $tablename FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($sql_fields) SET $sql_set;";

    # -- Clean up tmp file
    rm $FOLDER/$tsvname;
    echo "Completed import to $tablename";

done
 


