import sys, os, MySQLdb, json
from collections import defaultdict

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA_DB_USER"], 
                        passwd=os.environ["DATAVIVA_DB_PW"], 
                        db=os.environ["DATAVIVA_DB_NAME"])
db.autocommit(1)
cursor = db.cursor()

output = []
cursor.execute("select title_en, name_en, filter1, filter2 from apps_build, apps_app where app_id=apps_app.id order by title_en")

builds = defaultdict(list)
for r in cursor.fetchall():
    builds[r[0]].append(r)
for b_name, b in builds.items():
    data = {
        "build_title": b_name, 
        "apps": [], 
        "filters": set(["bra"])
    }
    for bb in b:
        if "Compare" in bb[1]:
            compare_build = {
                "build_title": b_name.replace("<bra>", "<bra> and <bra>"), 
                "apps": [bb[1]], 
                "filters": ["bra", "bra"]
            }
            if "<" in bb[2]:
                compare_build["filters"].append(bb[2].replace(">", "").replace("<", ""))
            if "<" in bb[3]:
                compare_build["filters"].append(bb[3].replace(">", "").replace("<", ""))
            output.append(compare_build)
        else:
            data["apps"].append(bb[1])
            if "<" in bb[2]:
                data["filters"].add(bb[2].replace(">", "").replace("<", ""))
            if "<" in bb[3]:
                data["filters"].add(bb[3].replace(">", "").replace("<", ""))
    data["filters"] = list(data["filters"])
    output.append(data)

j = json.dumps(output, indent=4)
f = open('dataviva_builds_new.json', 'w')
print >> f, j
f.close()