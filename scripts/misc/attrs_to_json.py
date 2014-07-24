import sys, os, MySQLdb, json

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

data = {}

cursor.execute("select cbo_id, name_en, avg(num_emp) from rais_yo, attrs_cbo where id=cbo_id group by cbo_id;")
data["cbo"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select isic_id, name_en, avg(num_emp) from rais_yi, attrs_isic where id=isic_id group by isic_id;")
data["isic"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select bra_id, name_en, avg(population) from attrs_yb, attrs_bra where id=bra_id group by bra_id")
data["bra"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select hs_id, name_en, avg(val_usd) from secex_yp, attrs_hs where id=hs_id group by hs_id;")
data["hs"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select wld_id, name_en, avg(val_usd) from secex_yw, attrs_wld where id=wld_id group by wld_id;")
data["wld"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

j = json.dumps(data, indent=4)
f = open('dataviva_attrs.json', 'w')
print >> f, j
f.close()