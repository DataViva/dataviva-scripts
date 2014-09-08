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

cursor.execute("select bra_id, name_en, avg(population) from attrs_yb, attrs_bra where id=bra_id group by bra_id")
data["bra"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select hs_id, name_en, avg(val_usd) from secex_yp, attrs_hs where id=hs_id group by hs_id;")
data["hs"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select wld_id, name_en, avg(val_usd) from secex_yw, attrs_wld where id=wld_id group by wld_id;")
data["wld"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA2_DB_USER"], 
                        passwd=os.environ["DATAVIVA2_DB_PW"], 
                        db=os.environ["DATAVIVA2_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

cursor.execute("select cnae_id, name_en, avg(num_emp) from rais_yi, attrs_cnae where id=cnae_id group by cnae_id;")
data["cnae"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select inst_id, name_en, sum(people) from lattes_bri, attrs_inst where id=inst_id group by inst_id having sum(people) >= 15")
data["inst"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select research_id, name_en, sum(people) from lattes_bri, attrs_research where id=research_id group by research_id")
data["research"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select grade_id, name_en, sum(enrolled) from sc_ybge, attrs_grade where id=grade_id and year = 2012 group by grade_id")
data["grade"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select ethnicity_id, name_en, sum(enrolled) from sc_ybge, attrs_ethnicity where id=ethnicity_id and year = 2012 group by ethnicity_id")
data["ethnicity"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]

cursor.execute("select course_id, name_en, sum(enrolled) from hedu_ybuc, attrs_course where id=course_id and year = 2012 group by course_id")
data["course"] = [{"id":x[0], "name":x[1], "weight":float(x[2])} for x in cursor.fetchall()]


j = json.dumps(data, indent=4)
f = open('dataviva_attrs.json', 'w')
print >> f, j
f.close()