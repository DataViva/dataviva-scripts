#!/bin/bash

if [ $2 = "yb" ]; then
  file=yb_diversity.tsv.bz2
  fields=(bra_id year wage num_emp num_est wage_avg num_emp_est isic_diversity isic_diversity_eff cbo_diversity cbo_diversity_eff)
  if [ $1 -gt "2002" ]; then
    file=yb_diversity_growth.tsv.bz2
    fields=(bra_id year wage num_emp num_est wage_avg num_emp_est isic_diversity isic_diversity_eff cbo_diversity cbo_diversity_eff wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct)
  fi
  if [ $1 -gt "2006" ]; then
    file=yb_diversity_growth.tsv.bz2
    fields=(bra_id year wage num_emp num_est wage_avg num_emp_est isic_diversity isic_diversity_eff cbo_diversity cbo_diversity_eff wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct wage_growth_val_5 wage_growth_pct_5 num_emp_growth_val_5 num_emp_growth_pct_5)
  fi
fi


if [ $2 = "yi" ]; then
  file=yi_diversity.tsv.bz2
  fields=(isic_id year wage num_emp num_est wage_avg num_emp_est cbo_diversity cbo_diversity_eff bra_diversity bra_diversity_eff)
  if [ $1 -gt "2002" ]; then
    file=yi_diversity_growth.tsv.bz2
    fields=(isic_id year wage num_emp num_est wage_avg num_emp_est cbo_diversity cbo_diversity_eff bra_diversity bra_diversity_eff wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct)
  fi
  if [ $1 -gt "2006" ]; then
    file=yi_diversity_growth.tsv.bz2
    fields=(isic_id year wage num_emp num_est wage_avg num_emp_est cbo_diversity cbo_diversity_eff bra_diversity bra_diversity_eff wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct wage_growth_val_5 wage_growth_pct_5 num_emp_growth_val_5 num_emp_growth_pct_5)
  fi
fi


if [ $2 = "yo" ]; then
  file=yo_diversity.tsv.bz2
  fields=(cbo_id year wage num_emp num_est wage_avg num_emp_est isic_diversity isic_diversity_eff bra_diversity bra_diversity_eff)
  if [ $1 -gt "2002" ]; then
    file=yo_diversity_growth.tsv.bz2
    fields=(cbo_id year wage num_emp num_est wage_avg num_emp_est isic_diversity isic_diversity_eff bra_diversity bra_diversity_eff wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct)
  fi
  if [ $1 -gt "2006" ]; then
    file=yo_diversity_growth.tsv.bz2
    fields=(cbo_id year wage num_emp num_est wage_avg num_emp_est isic_diversity isic_diversity_eff bra_diversity bra_diversity_eff wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct wage_growth_val_5 wage_growth_pct_5 num_emp_growth_val_5 num_emp_growth_pct_5)
  fi
fi




if [ $2 = "ybi" ]; then
  file=ybi_rcas_dist_opp.tsv.bz2
  fields=(year bra_id isic_id wage num_emp num_est wage_avg num_emp_est rca distance opp_gain)
  if [ $1 -gt "2002" ]; then
    file=ybi_rcas_dist_opp_growth.tsv.bz2
    fields=(bra_id isic_id year wage num_emp num_est wage_avg num_emp_est rca distance opp_gain wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct)
  fi
  if [ $1 -gt "2006" ]; then
    file=ybi_rcas_dist_opp_growth.tsv.bz2
    fields=(bra_id isic_id year wage num_emp num_est wage_avg num_emp_est rca distance opp_gain wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct wage_growth_val_5 wage_growth_pct_5 num_emp_growth_val_5 num_emp_growth_pct_5)
  fi
fi


if [ $2 = "ybo" ]; then
  file=ybo.tsv.bz2
  fields=(year bra_id cbo_id wage num_emp num_est wage_avg num_emp_est)
  if [ $1 -gt "2002" ]; then
    file=ybo_growth.tsv.bz2
    fields=(bra_id cbo_id year wage num_emp num_est wage_avg num_emp_est wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct)
  fi
  if [ $1 -gt "2006" ]; then
    file=ybo_growth.tsv.bz2
    fields=(bra_id cbo_id year wage num_emp num_est wage_avg num_emp_est wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct wage_growth_val_5 wage_growth_pct_5 num_emp_growth_val_5 num_emp_growth_pct_5)
  fi
fi


if [ $2 = "yio" ]; then
  file=yio_importance.tsv.bz2
  fields=(year isic_id cbo_id wage num_emp num_est wage_avg num_emp_est importance)
  if [ $1 -gt "2002" ]; then
    file=yio_importance_growth.tsv.bz2
    fields=(isic_id cbo_id year wage num_emp num_est wage_avg num_emp_est importance wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct)
  fi
  if [ $1 -gt "2006" ]; then
    file=yio_importance_growth.tsv.bz2
    fields=(isic_id cbo_id year wage num_emp num_est wage_avg num_emp_est importance wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct wage_growth_val_5 wage_growth_pct_5 num_emp_growth_val_5 num_emp_growth_pct_5)
  fi
fi



if [ $2 = "ybio" ]; then
  file=ybio_required.tsv.bz2
  fields=(year bra_id isic_id cbo_id wage num_emp num_est wage_avg num_emp_est required)
  if [ $1 -gt "2002" ]; then
    file=ybio_required_growth.tsv.bz2
    fields=(bra_id isic_id cbo_id year wage num_emp num_est wage_avg num_emp_est required wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct)
  fi
  if [ $1 -gt "2006" ]; then
    file=ybio_required_growth.tsv.bz2
    fields=(bra_id isic_id cbo_id year wage num_emp num_est wage_avg num_emp_est required wage_growth_val wage_growth_pct num_emp_growth_val num_emp_growth_pct wage_growth_val_5 wage_growth_pct_5 num_emp_growth_val_5 num_emp_growth_pct_5)
  fi
fi



file=$DATA_DIR"rais/$1/$file"
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

if [[ -z $DATAVIVA_DB_PW ]]; then
  mysql -u $DATAVIVA_DB_USER -e "load data local infile '$file' into table rais_$2 fields terminated by '\t' lines terminated by '\n' IGNORE 1 LINES $sql_fields SET $sql_set" dataviva --local-infile=1
else
  mysql -u $DATAVIVA_DB_USER -p $DATAVIVA_DB_PW -e "load data local infile '$file' into table rais_$2 fields terminated by '\t' lines terminated by '\n' IGNORE 1 LINES $sql_fields SET $sql_set" dataviva --local-infile=1
fi

rm $file