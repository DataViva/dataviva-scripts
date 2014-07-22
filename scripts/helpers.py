# -*- coding: utf-8 -*-
"""
    Helpers
    ~~~~~~~
    
"""

''' Import statements '''
import sys, bz2, gzip, zipfile,  os
from decimal import Decimal, ROUND_HALF_UP
from os.path import splitext, basename, exists

'''
    Used for finding environment variables through configuration
    if a default is not given, the site will raise an exception
'''
def get_env_variable(var_name, default=-1):
    try:
        return os.environ[var_name]
    except KeyError:
        if default != -1:
            return default
        error_msg = "Set the %s os.environment variable" % var_name
        raise Exception(error_msg)

def d(x):
  return Decimal(x).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)

def get_file(full_path):
    file_name = basename(full_path)
    file_path_no_ext, file_ext = splitext(file_name)

    extensions = {
        '.bz2': bz2.BZ2File,
        '.gz': gzip.open,
        '.zip': zipfile.ZipFile,
    }
    
    try:
        file = extensions[file_ext](full_path)
    except KeyError:
        file = open(full_path)
    except IOError:
        return None
    
    if file_ext == '.zip':
        file = zipfile.ZipFile.open(file, file_path_no_ext)
    
    # print "Reading from file", file_name
    return file

def format_runtime(x):
    # convert to hours, minutes, seconds
    m, s = divmod(x, 60)
    h, m = divmod(m, 60)
    if h:
        return "{0} hours and {1} minutes".format(int(h), int(m))
    if m:
        return "{0} minutes and {1} seconds".format(int(m), int(s))
    if s < 1:
        return "< 1 second"
    return "{0} seconds".format(int(s))
        