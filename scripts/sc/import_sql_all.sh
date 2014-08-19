#!/bin/bash

: '
CREATE TABLE `edu_ybge` (
  `year` int(4) unsigned NOT NULL,
  `bra_id` varchar(8) DEFAULT NULL,
  `grade` varchar(8) DEFAULT NULL,
  `ethnicity` varchar(7) DEFAULT NULL,
  `classes` int(11) DEFAULT NULL,
  `enrolled` int(11) DEFAULT NULL,
  `age` float DEFAULT NULL,
  `enrolled_f` int(11) DEFAULT NULL,
  `age_f` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

USAGE: bash scripts/edu/import_sql_all.sh data/edu/ year
'

dir=$1

tbls=(ybge)

years=($2)
if [ -z "$2" ]; then
  years=(2007 2008 2009 2010 2011 2012)
fi

for year in ${years[*]}
do
  for tbl in ${tbls[*]}
  do
    
    file=$dir$year"/$tbl.tsv.bz2"
    
    if [ $tbl = "ybge" ]; then
       fields=(year bra_id grade ethnicity schools age classes enrolled enrolled_f)
    fi
    
    sql_fields="("
    sql_set=""

    for field in ${fields[*]}
    do
      sql_fields+="@v$field, "
      sql_set+="$field = nullif(@v$field,''), "
    done

    sql_set=${sql_set%", "}
    sql_fields=${sql_fields%", "}
    sql_fields+=") "
  
    if [ ! -f $file ]; then
        echo "File not found!"
    fi

    bunzip2 -k $file
    file=${file%".bz2"}
    echo $file

    if [[ -z $DATAVIVA2_DB_PW ]]; then
      mysql -u $DATAVIVA2_DB_USER -e "load data local infile '$file' into table edu_$tbl fields terminated by '\t' lines terminated by '\n' IGNORE 1 LINES $sql_fields SET $sql_set" dataviva2 --local-infile=1
    else
      mysql -u $DATAVIVA2_DB_USER -p $DATAVIVA2_DB_PW -e "load data local infile '$file' into table edu_$tbl fields terminated by '\t' lines terminated by '\n' IGNORE 1 LINES $sql_fields SET $sql_set" dataviva2 --local-infile=1
    fi

    rm $file
    
  done
  # echo $file $year
done