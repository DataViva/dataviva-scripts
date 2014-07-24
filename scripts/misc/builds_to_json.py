import sys, os, MySQLdb, json
from collections import defaultdict

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

data = []
cursor.execute("select title_en, name_en, filter1, filter2 from apps_build, apps_app where app_id=apps_app.id order by title_en")
last = {"build_title":None}
for r in cursor.fetchall():
    # if last is None:
    #     last = {
    #         "build_title": r[0],
    #         "apps": [r[1]],
    #         "filters": set(["bra"])
    #     } 
    if r[0] != last["build_title"]:
        if last["build_title"] is not None:
            last["filters"] = list(last["filters"])
            data.append(last)
        last = {
            "build_title": r[0],
            "apps": [],
            "filters": set(["bra"])
        }
    last["apps"].append(r[1])
    if "<" in r[2]:
        last["filters"].add(r[2].replace(">", "").replace("<", ""))
    if "<" in r[3]:
        last["filters"].add(r[3].replace(">", "").replace("<", ""))

j = json.dumps(data, indent=4)
f = open('dataviva_builds.json', 'w')
print >> f, j
f.close()