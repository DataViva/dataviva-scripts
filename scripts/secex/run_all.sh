set -x
for i in `seq 2000 2013`;
do
  
  PREV_YEAR=`expr $i - 1`
  PREV_YEAR_FIVE=`expr $i - 5`
  
  python -m scripts.secex.step_1_aggregate \
      -y $i \
      data/secex/export/MDIC_$i.csv.zip

  python -m scripts.secex.step_2_disaggregate \
      -y $i \
      data/secex/export/$i/ybpw.tsv.bz2 \
      data/secex/export/$i/

  python -m scripts.secex.step_3_pci_wld_eci \
      -y $i \
      -d \
      -e data/secex/observatory_ecis.csv \
      -p data/secex/observatory_pcis.csv \
      data/secex/export/$i

  python -m scripts.secex.step_4_eci \
      -y $i \
      -d \
      data/secex/export/$i

  python -m scripts.secex.step_5_diversity \
      -y $i \
      -d \
      data/secex/export/$i

  python -m scripts.secex.step_6_yp_rca \
      -y $i \
      -d \
      data/secex/export/$i

  python -m scripts.secex.step_7_ybp_rdo \
      -y $i \
      -d \
      data/secex/export/$i

  if [ $i -eq 2000 ]
  then
    python -m scripts.secex.step_8_growth \
        data/secex/export/$i \
        -y $i \
        -t all \
        -d
  else
    if [ $i -ge 2005 ]
    then
      python -m scripts.secex.step_8_growth \
          data/secex/export/$i \
          -y $i \
          -t all \
          -d \
          -g data/secex/export/$PREV_YEAR \
          -g5 data/secex/export/$PREV_YEAR_FIVE
    else
      python -m scripts.secex.step_8_growth \
          data/secex/export/$i \
          -y $i \
          -t all \
          -d \
          -g data/secex/export/$PREV_YEAR 
    fi
  fi

done