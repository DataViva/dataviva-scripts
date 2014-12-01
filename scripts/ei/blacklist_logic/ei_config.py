# ei_config.py

import os
import click
import ntpath
import pandas as pd
import MySQLdb
import numpy as np
from pandas.tools.pivot import pivot_table

from table_aggregator import make_table

PURCHASES = 1
TRANSFERS = 2
DEVOLUTIONS = 3
CREDITS = 4
REMITS = 5


# -- Load in metadata from DB
print "Getting municipal data from DB..."
db = MySQLdb.connect(host=os.environ["DATAVIVA2_DB_HOST"], user=os.environ["DATAVIVA2_DB_USER"], 
                     passwd=os.environ["DATAVIVA2_DB_PW"], 
                     db=os.environ["DATAVIVA2_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()
cursor.execute("select id_mdic, id_ibge, id from attrs_bra;")
lookup = {}
for mdic, ibge, bra_id in cursor.fetchall():
    lookup[str(mdic)] = bra_id
    lookup[str(ibge)] = bra_id

print "Getting Product code data from DB..."
cursor.execute("select substr(id, 3), id from attrs_hs where substr(id, 3) != '' and length(id) = 6;")
hs_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
hs_lookup["9991"] = "229999"
hs_lookup["9992"] = "229999"
hs_lookup["9998"] = "229999"
hs_lookup["9997"] = "229999"

cursor.execute("select substr(id,2,6), id from attrs_cnae;")
print "Getting CNAE data from DB..."
cnae_lookup = {str(r[0]):r[1] for r in cursor.fetchall()}
cursor.close()

BRA_UNREPORTED = '0XX000007'
CNAE_NO_INFO = 'x00'
CNAE_BLACKLISTED = 'x01'
CNAE_OTHER = 'x99'
HS_BLACKLISTED = 'XX0023'

def lookup_location(x):
    if x == '-1':
        return BRA_UNREPORTED
    if x == '4128625':
        x = '5200605'
    muni = lookup[x]
    if not muni.startswith("4mg"):
        muni = muni[:3] # -- outside MG only use state level
    return muni

def update_hs_id(old_hs_id):
    return hs_lookup[str(old_hs_id)]

def lookup_cnae(x):
    if x in ['1', '-1']:
        return CNAE_NO_INFO
    if x in ['2', '-2']:
        return CNAE_NO_INFO
    return cnae_lookup[str(x)]

cols = ["ncm", "hs_id",
        "EconomicAtivity_ID_CNAE_Receiver_5d",
        "cnae_id_r",
        "EconomicAtivity_ID_CNAE_Sender_5d",
        "cnae_id_s",
        "CFOP_ID",
        "Receiver_foreign",
        "Sender_foreign",
        "bra_id_r",
        "bra_id_s",
        "year",
        "month",
        "transportation_cost",
        "ICMS_ST_Value",
        "ICMS_Value",
        "IPI_Value",
        "PIS_Value",
        "COFINS_Value",
        "II_Value",
        "product_value",
        "ISSQN_Value"]

converters = {"hs_id": update_hs_id, "bra_id_s":lookup_location, "bra_id_r":lookup_location, "cnae_id_r": lookup_cnae, 
            "cnae_id_s":lookup_cnae} 