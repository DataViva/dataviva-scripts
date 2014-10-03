# _demographic_calc.py
import pandas as pd

def compute_demographics(df, t_name="ybucd"):
    mynewtable = pd.DataFrame()     

    lookup = {"b":"bra_id", "c":"course_id", "u":"university_id", "d": "d_id", "y": "year"}
    
    mynewtable = pd.DataFrame()

    for d in range(3):
                
        my_pk = [ df[lookup[x]] for x in t_name if x != 'd' ]        
        my_pk.append( df["d_id"].str.get(d) )
        
        moi = df.groupby(my_pk, sort=False).sum()
        
        mynewtable = pd.concat([mynewtable, moi])
        print "done ", d

    return mynewtable