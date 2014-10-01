# demographics.py
import numpy as np

def map_gender(x):
    MALE, FEMALE = 0, 1
    gender_dict = {MALE: 'A', FEMALE: 'B'}
    if x in gender_dict:
        return str(gender_dict[x])
    return 'z'

def map_ethnicity(x):
    WHITE, BLACK, MULTI, ASIAN, INDIAN, NODATA = 1,2,3,4,5,6
    NOTREPORTED=0
    color_dict = {INDIAN:'C', WHITE:'D', BLACK:'E', ASIAN:'F', MULTI:'G', NODATA:'H', NOTREPORTED:'H' }
    c = int(x)
    if not c in  color_dict:
        print "WTF?", c
    return str(color_dict[c])

def map_color(color):
    INDIAN, WHITE, BLACK, ASIAN, MULTI, UNKNOWN = 1,2,4,6,8,9
    color_dict = {INDIAN:'C', WHITE:'D', BLACK:'E', ASIAN:'F', MULTI:'G', 9:'H', -1:'H' }
    c = int(color)
    return str(color_dict[c])

def map_age(age):
    age_bucket = int(np.floor( int(age) / 10 ))
    if age_bucket == 0: 
        age_bucket = 1
    elif age_bucket > 6:
        age_bucket = 6
    return str(age_bucket)

def map_literacy(lit):
    ILLITERATE, BASIC, HIGHSCHOOL, COLLEGE, UNKNOWN = 1, 2, 3, 4, 9
    lit_dict = {1:'I', 2:'I', 3:'J', 4:'J', 5:'J', 6:'J', 7:'K', 
                8:'K', 9:'L', -1:'M' }
    return str(lit_dict[int(lit)])