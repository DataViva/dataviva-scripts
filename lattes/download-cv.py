from Queue import Queue
from threading import Thread
import json
import urllib2
import codecs
import os

try: os.makedirs('data/cv')
except: pass

def worker():
  while True:
    cvid, name, id2, country = q.get()
    urlpath = 'http://buscatextual.cnpq.br/buscatextual/visualizacv.do?id=' + cvid
    response = urllib2.urlopen(urlpath)
    html = response.read().decode('ISO-8859-1') # read and decode the response
    f = codecs.open('data/cv/' + cvid + '.html', 'w', 'utf-8')
    f.write(html)
    f.close()
    q.task_done()

q = Queue()
num_worker_threads = 100
for i in range(num_worker_threads):
  t = Thread(target=worker)
  t.daemon = True
  t.start()

# load the people ids
f = open('data/people.json', 'r')
people = json.load(f)[206290:]
f.close()

for item in people: q.put(item)
q.join()       # block until all tasks are done
print 'all done!'
