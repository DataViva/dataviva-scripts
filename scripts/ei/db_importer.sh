#!/bin/bash
# CREATE TABLE `ei_ymbibip` (
#   `year` int(4) NOT NULL,
#   `month` int(2) NOT NULL,
#   `sender_bra_id` varchar(8) NOT NULL,
#   `sender_cnae_id` varchar(5) NOT NULL,
#   `receiver_bra_id` varchar(8) NOT NULL,
#   `receiver_cnae_id` varchar(5) NOT NULL,
#   `hs_id` varchar(4) NOT NULL,
#   `origin` varchar(1) NOT NULL,
#   `product_value` float DEFAULT NULL,
#   `tax` float DEFAULT NULL,
#   `icms_tax` float DEFAULT NULL,
#   `transportation_cost` float NULL
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

set -x

if [ -z "$1" ]
  then
    echo "** ERROR: No directory supplied. Please path an absolute folder path.";
    exit -1;
fi



# -- one parameter required: path to folder
FOLDER=$1

for fullpath in $FOLDER/output_2013_0*.csv
do
    tablename="ei_ymbibip"
    echo "Importing $fullpath to SQL table $tablename";

    fields="year, month, sender_bra_id, sender_cnae_id, receiver_bra_id, receiver_cnae_id, hs_id, origin,\
    		product_value, tax, icms_tax, transportation_cost";
    mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields);"

    echo "Completed import to $tablename";

done
 


#mysql -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '$fullpath' INTO TABLE $tablename FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES  ($fields)\
# SET product_value = CONVERT(replace(@pvalue,',', '.'), DECIMAL(10)),sender_bra_id = (SELECT id from attrs_bra WHERE id_ibge = @sender_bra), receiver_bra_id = (SELECT id from attrs_bra WHERE id_ibge = @receiver_bra);";

