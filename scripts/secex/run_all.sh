python -m scripts.secex.step_1_aggregate \
    -y 2001 \
    data/secex/export/MDIC_2001.csv.zip

python -m scripts.secex.step_2_disaggregate \
    -y 2001 \
    data/secex/export/2001/ybpw.tsv.bz2

python -m scripts.secex.step_3_pci_wld_eci \
    -y 2001 \
    -d \
    -e data/secex/observatory_ecis.csv \
    -p data/secex/observatory_pcis.csv \
    data/secex/export/2001

python -m scripts.secex.step_4_eci \
    -y 2001 \
    -d \
    data/secex/export/2001

python -m scripts.secex.step_5_diversity \
    -y 2001 \
    -d \
    data/secex/export/2001

python -m scripts.secex.step_6_yp_rca \
    -y 2001 \
    -d \
    data/secex/export/2001

python -m scripts.secex.step_7_ybp_rdo \
    -y 2001 \
    -d \
    data/secex/export/2001

python -m scripts.secex.step_8_growth \
    -y 2001 \
    -t all \
    -d \
    data/secex/export/2001 \
    data/secex/export/2000
