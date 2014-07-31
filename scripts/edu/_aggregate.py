import sys
import pandas as pd
import numpy as np

def aggregate(df):
    # split by gender
    df_women = df[df["gender"]==0]
    df_men = df[df["gender"]==1]
    
    df = df.drop(["gender"], axis=1)
    df = df.rename(columns={"munic":"bra_id", "enroll_id":"enrolled", "student_id":"students","class_id":"classes"})
    ybsc = df.groupby(["year", "bra_id", "school_id", "course_id"]) \
            .agg({"enrolled": pd.Series.nunique, "students": pd.Series.nunique, "classes": pd.Series.nunique, "age": np.sum})
    
    df_men = df_men.drop(["gender", "class_id"], axis=1)
    df_men = df_men.rename(columns={"munic":"bra_id", "enroll_id":"enrolled_m", "student_id":"students_m", "age":"age_m"})
    ybsc_m = df_men.groupby(["year", "bra_id", "school_id", "course_id"]) \
            .agg({"enrolled_m": pd.Series.nunique, "students_m": pd.Series.nunique, "age_m":np.sum})
    
    df_women = df_women.drop(["gender", "class_id"], axis=1)
    df_women = df_women.rename(columns={"munic":"bra_id", "enroll_id":"enrolled_f", "student_id":"students_f", "age":"age_f"})
    ybsc_w = df_women.groupby(["year", "bra_id", "school_id", "course_id"]) \
            .agg({"enrolled_f": pd.Series.nunique, "students_f": pd.Series.nunique, "age_f":np.sum})
    
    ybsc = ybsc.merge(ybsc_m, how="outer", left_index=True, right_index=True)
    ybsc = ybsc.merge(ybsc_w, how="outer", left_index=True, right_index=True)
    
    '''
        BRA AGGREGATIONS
    '''
    ybsc_state = ybsc.reset_index()
    ybsc_state["bra_id"] = ybsc_state["bra_id"].apply(lambda x: x[:2])
    ybsc_state = ybsc_state.groupby(["year", "bra_id", "school_id", "course_id"]).sum()

    ybsc_meso = ybsc.reset_index()
    ybsc_meso["bra_id"] = ybsc_meso["bra_id"].apply(lambda x: x[:4])
    ybsc_meso = ybsc_meso.groupby(["year", "bra_id", "school_id", "course_id"]).sum()
   
    ybsc = pd.concat([ybsc, ybsc_state, ybsc_meso])
    
    '''
        Course AGGREGATIONS
    '''
    ybsc_course_2 = ybsc.reset_index()
    ybsc_course_2["course_id"] = ybsc_course_2["course_id"].apply(lambda x: x[:2])
    ybsc_course_2 = ybsc_course_2.groupby(["year", "bra_id", "school_id", "course_id"]).sum()
   
    ybsc = pd.concat([ybsc, ybsc_course_2])
    
    return ybsc