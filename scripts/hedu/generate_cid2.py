#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import sys, MySQLdb, os
import pandas as pd
import numpy as np
import re

db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                            passwd=os.environ["DATAVIVA_DB_PW"], 
                            db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)

missing_bras = {}


cursor = db.cursor()
    
cursor.execute("select id from attrs_course_hedu")
courses = [r[0] for r in cursor.fetchall()]

count = 0
for c in courses:
    try:
        cid2 = c[:2]
        cursor.execute("INSERT INTO attrs_course_hedu VALUES ('%s', 'UNDEFINED', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);" % (cid2))
        count +=1
    except Exception,e:
        x,y = e
        if x!=1062:
            print e
# print courses
print count, "added"

# # res = df[df.School_ID == sid].values[0][1].encode('punycode')
# # print sid, "=", res

# df = pd.read_csv("/Users/jspeiser/Downloads/School_names.csv", sep=";")
# df.School_ID = df.School_ID.astype(str)
# # print df.head()
# count = 0

# # for sid in missing_ids:
# for i,x in df.iterrows():
#     # print x
#     sid = x.values[0]
#     res = x.values[1].encode('punycode')

# print "Added", count, "things"