import sys, os, MySQLdb, json

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

data = {}

cursor.execute("select distinct cbo_id from rais_yo;")
data["cbo"] = [x[0] for x in cursor.fetchall()]

cursor.execute("select distinct isic_id from rais_yi limit 0, 5;")
data["isic"] = [x[0] for x in cursor.fetchall()]

cursor.execute("select distinct bra_id from rais_yb;")
data["bra"] = [x[0] for x in cursor.fetchall()]

cursor.execute("select distinct hs_id from secex_yp;")
data["hs"] = [x[0] for x in cursor.fetchall()]

cursor.execute("select distinct wld_id from secex_yw;")
data["wld"] = [x[0] for x in cursor.fetchall()]

cursor.execute("select distinct bra_id from secex_yb;")
data["bra"] = list(set(data["bra"]).union([x[0] for x in cursor.fetchall()]))

for a in data.keys():
    q = 'SELECT id, name_en FROM attrs_{0} WHERE id IN (%s)'.format(a)
    in_p = ', '.join(list(map(lambda x: '%s', data[a])))
    q = q % in_p
    cursor.execute(q, data[a])
    data[a] = [{"id":x[0], "name":x[1]} for x in cursor.fetchall()]

j = json.dumps(data, indent=4)
f = open('dataviva_attrs.json', 'w')
print >> f, j
f.close()