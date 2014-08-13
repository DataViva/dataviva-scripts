#!/bin/sh
set -x
START_YEAR=2002
FIVE_YEAR_THRESHOLD=START_YEAR+4
YEAR=2002
MAX_YEAR=2005

# -- Variables that need to be set

if [[ -z "$1" || -z "$2" || -z "$3" ]] 
  then
    echo "** ERROR: Not all parameters supplied";
    echo "Usage: ./tsv_generator.sh <PATH TO DATAVIVA SCRIPT FOLDER> <DESIRED OUTPUT PATH> <PATH OF ORIGINAL RAIS DATA FOLDER>";
    exit -1;
fi

DV_SCRIPT_PATH=$1
OUTPUT_PATH=$2
ORIGINAL_DATA_PATH=$3

until [  $YEAR -gt $MAX_YEAR ]; do
    let "PREV_YEAR=YEAR-1"
    let "PREV_YEAR_5=YEAR-5"
    GROWTH=""
    GROWTH_5=""

    echo "Year: $YEAR, Prev: $PREV_YEAR, Prev5: $PREV_YEAR_5";
    if [  $YEAR -gt $START_YEAR ]; then
        GROWTH="-g $OUTPUT_PATH/$PREV_YEAR"
    fi

    if [  $YEAR -gt $FIVE_YEAR_THRESHOLD ]; then
        GROWTH_5="-g5 $OUTPUT_PATH/$PREV_YEAR_5"
    fi

    cd $DV_SCRIPT_PATH

    python -m scripts.rais_new.format_raw_data "$ORIGINAL_DATA_PATH/Rais_$YEAR.csv.rar" -y $YEAR \
       -o "$OUTPUT_PATH/$YEAR" $GROWTH  $GROWTH_5  -d

    let "YEAR+=1"
done
    

