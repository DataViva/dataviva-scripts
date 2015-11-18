#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No arguments provided, need a year"
    exit 1
fi

PWD=/${PWD#*/}

python scripts/secex_monthly/format_raw_data.py \
          $PWD/data/secex/raw_export/MDIC_$1.rar \
          $PWD/data/secex/raw_import/MDIC_$1.rar \
          -y $1 \
          -e $PWD/data/comtrade/$1/comtrade_eci.tsv.bz2 \
          -p $PWD/data/comtrade/$1/comtrade_pci.tsv.bz2 \
          -r $PWD/data/comtrade/$1/comtrade_ypw.tsv.bz2 \
          -o $PWD/data/secex/$1

if [ $1 -gt "2000" ]; then
  PREV_YEAR=`expr $1 - 1`
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymb.tsv.bz2 $PWD/data/secex/$PREV_YEAR/ymb.tsv.bz2 --years=1 --cols=export_val,import_val -o $PWD/data/secex/$1
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymbp.tsv.bz2 $PWD/data/secex/$PREV_YEAR/ymbp.tsv.bz2 --years=1 --cols=export_val,import_val -o $PWD/data/secex/$1 -s hs_id
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymbpw.tsv.bz2 $PWD/data/secex/$PREV_YEAR/ymbpw.tsv.bz2 --years=1 --cols=export_val,import_val -o $PWD/data/secex/$1 -s hs_id
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymbw.tsv.bz2 $PWD/data/secex/$PREV_YEAR/ymbw.tsv.bz2 --years=1 --cols=export_val,import_val -o $PWD/data/secex/$1
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymp.tsv.bz2 $PWD/data/secex/$PREV_YEAR/ymp.tsv.bz2 --years=1 --cols=export_val,import_val -o $PWD/data/secex/$1 -s hs_id
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ympw.tsv.bz2 $PWD/data/secex/$PREV_YEAR/ympw.tsv.bz2 --years=1 --cols=export_val,import_val -o $PWD/data/secex/$1 -s hs_id
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymw.tsv.bz2 $PWD/data/secex/$PREV_YEAR/ymw.tsv.bz2 --years=1 --cols=export_val,import_val -o $PWD/data/secex/$1
fi
if [ $1 -gt "2004" ]; then
  PREV_YEAR_FIVE=`expr $1 - 5`
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymb.tsv.bz2 $PWD/data/secex/$PREV_YEAR_FIVE/ymb.tsv.bz2 --years=5 --cols=export_val,import_val -o $PWD/data/secex/$1
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymbp.tsv.bz2 $PWD/data/secex/$PREV_YEAR_FIVE/ymbp.tsv.bz2 --years=5 --cols=export_val,import_val -o $PWD/data/secex/$1 -s hs_id
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymbpw.tsv.bz2 $PWD/data/secex/$PREV_YEAR_FIVE/ymbpw.tsv.bz2 --years=5 --cols=export_val,import_val -o $PWD/data/secex/$1 -s hs_id
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymbw.tsv.bz2 $PWD/data/secex/$PREV_YEAR_FIVE/ymbw.tsv.bz2 --years=5 --cols=export_val,import_val -o $PWD/data/secex/$1
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymp.tsv.bz2 $PWD/data/secex/$PREV_YEAR_FIVE/ymp.tsv.bz2 --years=5 --cols=export_val,import_val -o $PWD/data/secex/$1 -s hs_id
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ympw.tsv.bz2 $PWD/data/secex/$PREV_YEAR_FIVE/ympw.tsv.bz2 --years=5 --cols=export_val,import_val -o $PWD/data/secex/$1 -s hs_id
  python $PWD/scripts/common/growth_calc.py $PWD/data/secex/$1/ymw.tsv.bz2 $PWD/data/secex/$PREV_YEAR_FIVE/ymw.tsv.bz2 --years=5 --cols=export_val,import_val -o $PWD/data/secex/$1
fi

python scripts/common/db_importer.py --idir=$PWD/data/secex/$1/ --name=secex