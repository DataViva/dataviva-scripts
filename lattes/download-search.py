import urllib2
import codecs
import os

try: os.makedirs('data')
except: pass

inc = 10000
offset = 0
limit = 211056
while (offset < 211057):
  urlpath = 'http://buscatextual.cnpq.br/buscatextual/busca.do?metodo=forwardPaginaResultados&registros=' + str(offset) + ';' + str(inc) + '&query=%28+%2Bidx_particao%3A1+%2Bidx_nacionalidade%3Ae%29+or+%28+%2Bidx_particao%3A1+%2Bidx_nacionalidade%3Ab%29&analise=cv&tipoOrdenacao=null&paginaOrigem=index.do&mostrarScore=false&mostrarBandeira=true&modoIndAdhoc=null'
  response = urllib2.urlopen(urlpath)
  html = response.read().decode('ISO-8859-1') # read and decode the response
  f = codecs.open('data/' + str(offset) + '-' + str(inc) + '.html', 'w', 'utf-8')
  f.write(html)
  f.close()
  offset += inc

exit(1)

prog = re.compile("abreDetalhe\((.*?)\)")
for a in prog.finditer(html):
  print a.group(1).split(',')


prog = re.compile("<img  alt='(.*?)'")
for a in prog.finditer(html):
  print a.group(1)
