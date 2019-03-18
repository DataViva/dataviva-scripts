#/bin/bash

echo "-------------------"
echo "2017"
echo ""
python format_raw_data.py SECEX/export/export_2017.csv SECEX/import/import_2017.csv -y 2017 -e COMTRADE/2017/comtrade_eci.tsv -p COMTRADE/2017/comtrade_pci.tsv -r COMTRADE/2017/comtrade_ypw.tsv -o output > output/output2017.txt

echo "-------------------"
echo "2016"
echo ""
python format_raw_data.py SECEX/export/export_2016.csv SECEX/import/import_2016.csv -y 2016 -e COMTRADE/2016/comtrade_eci.tsv -p COMTRADE/2016/comtrade_pci.tsv -r COMTRADE/2016/comtrade_ypw.tsv -o output > output/output2016.txt

echo "-------------------"
echo "2015"
echo ""
python format_raw_data.py SECEX/export/export_2015.csv SECEX/import/import_2015.csv -y 2015 -e COMTRADE/2015/comtrade_eci.tsv -p COMTRADE/2015/comtrade_pci.tsv -r COMTRADE/2015/comtrade_ypw.tsv -o output > output/output2015.txt

echo "-------------------"
echo "2014"
echo ""
python format_raw_data.py SECEX/export/export_2014.csv SECEX/import/import_2014.csv -y 2014 -e COMTRADE/2014/comtrade_eci.tsv -p COMTRADE/2014/comtrade_pci.tsv -r COMTRADE/2014/comtrade_ypw.tsv -o output > output/output2014.txt

echo "-------------------"
echo "2013"
echo ""
python format_raw_data.py SECEX/export/export_2013.csv SECEX/import/import_2013.csv -y 2013 -e COMTRADE/2013/comtrade_eci.tsv -p COMTRADE/2013/comtrade_pci.tsv -r COMTRADE/2013/comtrade_ypw.tsv -o output > output/output2013.txt

echo "-------------------"
echo "2012"
echo ""
python format_raw_data.py SECEX/export/export_2012.csv SECEX/import/import_2012.csv -y 2012 -e COMTRADE/2012/comtrade_eci.tsv -p COMTRADE/2012/comtrade_pci.tsv -r COMTRADE/2012/comtrade_ypw.tsv -o output > output/output2012.txt

echo "-------------------"
echo "2011"
echo ""
python format_raw_data.py SECEX/export/export_2011.csv SECEX/import/import_2011.csv -y 2011 -e COMTRADE/2011/comtrade_eci.tsv -p COMTRADE/2011/comtrade_pci.tsv -r COMTRADE/2011/comtrade_ypw.tsv -o output > output/output2011.txt

echo "-------------------"
echo "2010"
echo ""
python format_raw_data.py SECEX/export/export_2010.csv SECEX/import/import_2010.csv -y 2010 -e COMTRADE/2010/comtrade_eci.tsv -p COMTRADE/2010/comtrade_pci.tsv -r COMTRADE/2010/comtrade_ypw.tsv -o output > output/output2010.txt
