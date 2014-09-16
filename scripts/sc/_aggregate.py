import sys
import pandas as pd
import numpy as np

def aggregate(df):
    # split by gender
    df = df.rename(columns={"munic":"bra_id", "enroll_id":"enrolled", \
                            "edu_level_new":"grade", "school_id":"schools", \
                            "class_id":"classes", "color":"ethnicity"})
    
    df_f = df[df["gender"]==1]
    
    df = df.groupby(["year", "bra_id", "grade", "ethnicity"]).agg({"schools": pd.Series.nunique, "classes": pd.Series.nunique, "enrolled": pd.Series.nunique, "age":np.sum})
    df_f = df_f.groupby(["year", "bra_id", "grade", "ethnicity"]).agg({"enrolled": pd.Series.nunique})
    df_f = df_f.rename(columns={"enrolled":"enrolled_f"})
    
    ybge = df.merge(df_f, left_index=True, right_index=True, how="outer")

    '''
        BRA AGGREGATIONS
    '''
    ybge_state = ybge.reset_index()
    ybge_state["bra_id"] = ybge_state["bra_id"].apply(lambda x: x[:2])
    ybge_state = ybge_state.groupby(["year", "bra_id", "grade", "ethnicity"]).sum()

    ybge = pd.concat([ybge, ybge_state])
    
    ybge["age"] = ybge["age"] / ybge["enrolled"]

    return ybge