# coding=UTF-8
import re
import json
import codecs

inc = 10000
offset = 0
limit = 211056
people = []
while (offset < 211057):
  fpath = 'data/' + str(offset) + "-" + str(inc) + '.html'
  f = codecs.open(fpath, 'r', 'utf-8')
  html = f.read()
  prog = re.compile("abreDetalhe\('(.*?)','(.*?)',(.*?)\).*?<img  alt='(.*?)'", re.DOTALL)
  npeople = 0
  for a in prog.finditer(html):
    people.append((a.group(1), a.group(2), a.group(3), a.group(4)))
  offset += inc

f = open('data/people.json', 'w')
json.dump(people, f, indent=4)
f.close()

