from dataviva import db
from pandas import DataFrame
from dataviva.secex.models import NG_Ymbpw as Ymbpw
from math import pow

import time

EXPONENT5 = .2


def do_agg():
    query = Ymbpw.query.filter(Ymbpw.month == 0, Ymbpw.year.in_([2005,2009,2010]), Ymbpw.bra_id_len == 2, Ymbpw.hs_id == '01')
    results = query.all()
    lookup = { str(item.year) + item.bra_id + item.wld_id + item.hs_id : item.export_val  for item in results }

    for item in results:
        suffix = item.bra_id + item.wld_id + item.hs_id

        key1 = str( item.year - 1 ) + suffix
        key5 = str( item.year - 5 ) + suffix
        
        if key1 in lookup:
            val1 = lookup[key1]
            if val1:
                setattr(item,"growth_1" , (item.export_val/ lookup[key1]) - 1)
            elif not val1 and item.export_val:
                setattr(item, "growth_1", None)
    
        if key5 in lookup:
            val5 = lookup[key5]
            if val5:
                setattr(item, "growth_5",  pow(item.export_val / lookup[key5], EXPONENT5) - 1)
            elif not val5 and item.export_val:
                setattr(item, "growth_5", None)

    return results


start = time.time()
res = do_agg()
end = time.time()
#print res
for x in res:
    print x
print "took", (end-start), "seconds"

