set -x
for i in `seq 2002 2013`;
do
  PREV_YEAR=`expr $i - 1`
  PREV_YEAR_FIVE=`expr $i - 5`
  
  if [ $i -eq 2000 ]
  then
    python scripts/secex_new/format_raw_data.py \
      data/secex/export/MDIC_$i.rar \
      data/secex/import/MDIC_$i.rar \
      -y $i \
      -e data/secex/observatory_ecis.csv \
      -p data/secex/observatory_pcis.csv \
      -o data/secex/$i
  else
    if [ $i -ge 2005 ]
    then
      python scripts/secex_new/format_raw_data.py \
        data/secex/export/MDIC_$i.rar \
        data/secex/import/MDIC_$i.rar \
        -y $i \
        -e data/secex/observatory_ecis.csv \
        -p data/secex/observatory_pcis.csv \
        -o data/secex/$i \
        -g data/secex/$PREV_YEAR \
        -g5 data/secex/$PREV_YEAR_FIVE 
    else
      python scripts/secex_new/format_raw_data.py \
        data/secex/export/MDIC_$i.rar \
        data/secex/import/MDIC_$i.rar \
        -y $i \
        -e data/secex/observatory_ecis.csv \
        -p data/secex/observatory_pcis.csv \
        -o data/secex/$i \
        -g data/secex/$PREV_YEAR 
    fi
  fi

done