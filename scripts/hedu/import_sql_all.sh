#!/bin/bash

: '
CREATE TABLE `hedu_ybuc` (
  `year` int(4) unsigned NOT NULL,
  `bra_id` varchar(8) DEFAULT NULL,
  `university_id` varchar(8) DEFAULT NULL,
  `course_id` varchar(7) DEFAULT NULL,
  `openings` int(11) DEFAULT NULL,
  `enrolled` int(11) DEFAULT NULL,
  `graduates` int(11) DEFAULT NULL,
  `entrants` int(11) DEFAULT NULL,
  KEY `hedu_ybuc_ibfk_1` (`bra_id`),
  KEY `hedu_ybuc_ibfk_2` (`course_id`),
  CONSTRAINT `hedu_ybuc_ibfk_1` FOREIGN KEY (`bra_id`) REFERENCES `attrs_bra` (`id`),
  CONSTRAINT `hedu_ybuc_ibfk_2` FOREIGN KEY (`course_id`) REFERENCES `attrs_course_hedu` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `hedu_ybc` (
  `year` int(4) unsigned NOT NULL,
  `bra_id` varchar(8) DEFAULT NULL,
  `course_id` varchar(7) DEFAULT NULL,
  `enrolled_rca` float DEFAULT NULL,
  KEY `hedu_ybc_ibfk_1` (`bra_id`),
  KEY `hedu_ybc_ibfk_2` (`course_id`),
  CONSTRAINT `hedu_ybc_ibfk_1` FOREIGN KEY (`bra_id`) REFERENCES `attrs_bra` (`id`),
  CONSTRAINT `hedu_ybc_ibfk_2` FOREIGN KEY (`course_id`) REFERENCES `attrs_course_hedu` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

USAGE: bash scripts/higher_edu/import_sql_all.sh data/higher_edu/ year
'

dir=$1

tbls=(ybuc ybc)

years=($2)
if [ -z "$2" ]; then
  years=(2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012)
fi

for year in ${years[*]}
do
  for tbl in ${tbls[*]}
  do
    
    file=$dir$year"/$tbl.tsv.bz2"
    
    if [ $tbl = "ybuc" ]; then
       fields=(year bra_id university_id course_id openings enrolled graduates entrants)
    else
       fields=(year bra_id course_id enrolled_rca)
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
      mysql -u $DATAVIVA2_DB_USER -e "load data local infile '$file' into table hedu_$tbl fields terminated by '\t' lines terminated by '\n' IGNORE 1 LINES $sql_fields SET $sql_set" dataviva2 --local-infile=1
    else
      mysql -u $DATAVIVA2_DB_USER -p $DATAVIVA2_DB_PW -e "load data local infile '$file' into table hedu_$tbl fields terminated by '\t' lines terminated by '\n' IGNORE 1 LINES $sql_fields SET $sql_set" dataviva2 --local-infile=1
    fi

    rm $file
    
  done
  # echo $file $year
done