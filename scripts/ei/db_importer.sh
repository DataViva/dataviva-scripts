#!/bin/bash
set -x

if [ -z "$1" ]
  then
    echo "** ERROR: No directory supplied. Please path an absolute folder path.";
    exit -1;
fi

# -- one parameter required: path to folder
FOLDER=$1

for fullpath in $FOLDER/output_ymsrp_*.csv
do
    tablename="ei_ymsrp"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, bra_id_s, cnae_id_s, bra_id_r, cnae_id_r, hs_id, \
    		product_value, tax, icms_tax, transportation_cost";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";

done
 

# -- YMS table import

for fullpath in $FOLDER/output_yms_*.csv
do
    tablename="ei_yms"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, bra_id_s, cnae_id_s,\
    		product_value, tax, icms_tax, transportation_cost";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";

done

for fullpath in $FOLDER/output_ymr_*.csv
do
    tablename="ei_ymr"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, bra_id_r, cnae_id_r,\
    		product_value, tax, icms_tax, transportation_cost";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";

done
 
for fullpath in $FOLDER/output_ymp_*.csv
do
    tablename="ei_ymp"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, hs_id,\
    		product_value, tax, icms_tax, transportation_cost";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";

done

for fullpath in $FOLDER/output_ymrp_*.csv
do
    tablename="ei_ymrp"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, bra_id_r, cnae_id_r, hs_id, \
    		product_value, tax, icms_tax, transportation_cost";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";

done
 
for fullpath in $FOLDER/output_ymsp_*.csv
do
    tablename="ei_ymsp"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, bra_id_s, cnae_id_s, hs_id, \
    		product_value, tax, icms_tax, transportation_cost";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";
done

for fullpath in $FOLDER/output_ymsr_*.csv
do
    tablename="ei_ymsr"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, bra_id_s, cnae_id_s, bra_id_r, cnae_id_r, \
    		product_value, tax, icms_tax, transportation_cost";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";
done


#mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields)\
# SET product_value = CONVERT(replace(@pvalue,',', '.'), DECIMAL(10)),sender_bra_id = (SELECT id from attrs_bra WHERE id_ibge = @sender_bra), receiver_bra_id = (SELECT id from attrs_bra WHERE id_ibge = @receiver_bra);";
