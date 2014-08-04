set -x
for i in `seq 2000 2013`;
do
  PREV_YEAR=`expr $i - 1`
  PREV_YEAR_FIVE=`expr $i - 5`
  
  if [ $i -eq 2000 ]
  then
    python -m scripts.secex_new.format_raw_data \
      data/secex/export/MDIC_$i.csv.zip \
      data/secex/import/MDIC_$i.csv.zip \
      -y $i \
      -e data/secex/observatory_ecis.csv \
      -p data/secex/observatory_pcis.csv \
      -o data/secex/$i -d
  else
    if [ $i -ge 2005 ]
    then
      python -m scripts.secex_new.format_raw_data \
        data/secex/export/MDIC_$i.csv.zip \
        data/secex/import/MDIC_$i.csv.zip \
        -y $i \
        -e data/secex/observatory_ecis.csv \
        -p data/secex/observatory_pcis.csv \
        -o data/secex/$i \
        -g data/secex/$PREV_YEAR \
        -g5 data/secex/$PREV_YEAR_FIVE \
        -d
    else
      python -m scripts.secex_new.format_raw_data \
        data/secex/export/MDIC_$i.csv.zip \
        data/secex/import/MDIC_$i.csv.zip \
        -y $i \
        -e data/secex/observatory_ecis.csv \
        -p data/secex/observatory_pcis.csv \
        -o data/secex/$i \
        -g data/secex/$PREV_YEAR \
        -d
    fi
  fi

done