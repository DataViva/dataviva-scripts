# coding=UTF-8
import re
import json
import codecs

def scrape(html, regexp, gnums=[1]):
  exp = re.compile(regexp, re.DOTALL)
  return [[match.group(gnum) for gnum in gnums] for match in exp.finditer(html)]
  
def getCiteNames(html):
  res = scrape(html, u'Nome em citações.*?<div class="layout-cell-pad-5">(.*?)</div>')
  if len(res) > 0: return res[0][0].split(';')
  else: return None

def getName(html):
  res = scrape(html, u'<TITLE>.*?\((.*?)\)</TITLE>')
  if len(res) > 0: return res[0][0]
  else: return None

def getAffil(html):
  res = scrape(html, u'Endereço Profissional.*?<div class="layout-cell-pad-5">(.*?)</div>')
  if len(res) > 0: return res[0][0].split('<br class="clear" />')
  else: return None

def getAreas(html):
  tmp = scrape(html, u'<a name="AreasAtuacao">(.*?)<a name="')[0][0]
  tmp = scrape(tmp, u'<b>[0-9]+. </b>.*?<div class="layout-cell-pad-5">(.*?)<br class="clear" />')
  results = []
  for a in tmp:
    grandarea = scrape(a[0], u'Grande área: (.*?)( /|\.)')
    area = scrape(a[0], u'Área: (.*?)( /|\.)')
    subarea = scrape(a[0], u'Subárea: (.*?)( /|\.)')
    result = {}
    if len(grandarea) > 0: result['grandarea'] = grandarea[0]
    if len(area) > 0: result['area'] = area[0]
    if len(subarea) > 0: result['subarea'] = subarea[0]
    results.append(result)
  return results
cvid = 'K4787894Z8'
f = codecs.open('data/cv/' + cvid + ".html", 'r', 'utf-8')
html = f.read()
f.close()
person = {}
person['cite-names'] = getCiteNames(html)
person['name'] = getName(html)
person['affil'] = getAffil(html)
person['areas'] = getAreas(html)
print json.dumps(person, indent=4, ensure_ascii=False)
