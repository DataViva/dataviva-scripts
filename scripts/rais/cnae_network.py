import sys, os, click, MySQLdb, csv

''' Connect to DB '''
db = MySQLdb.connect(host="localhost", 
                     user="root", 
                     db="dataviva2")
db.autocommit(1)
cursor = db.cursor()

def frange(x, y, jump):
    while x < y:
        yield x
        x += jump

@click.command()
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main(output_path):
    cursor.execute("select distinct cnae_id from rais_ii where length(cnae_id) = 6;")
    cnaes = [r[0] for r in cursor.fetchall()]

    edges = set([])
    for cnae in cnaes:
        nada = True
        cutoffs = list(frange(0.4, 0.75, 0.01))
        cutoffs.reverse()
        # for cutoff in [0.8, 0.75, 0.725, 0.7, 0.675, 0.65, 0.6, 0.55, 0.5]:
        for cutoff in cutoffs:
            cursor.execute("select * from rais_ii where cnae_id = %s and proximity < 1 and proximity >= %s;", [cnae, cutoff])
            conns = [list(c) for c in cursor.fetchall()]
            if len(conns) >= 3:
                # print c, len(conns)
                for c in conns:
                    c.sort(reverse=True)
                    edges.add(tuple(c))
                # edges += conns
                nothing = False
                break
            # if nothing:
    
    # print len(edges)
    
    with open(os.path.join(output_path,'cnae_network.tsv'), 'w') as fp:
        a = csv.writer(fp, delimiter='\t')
        a.writerow(('cnae_source', 'cnae_target', 'proximity'))
        a.writerows(edges)

if __name__ == "__main__":
    main()