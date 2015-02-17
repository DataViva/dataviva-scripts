# -*- coding: utf-8 -*-
import os, sys, time, bz2, click
import pandas as pd
import pandas.io.sql as sql
import numpy as np
import itertools

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
def main(file_path, output_path):
    nestings = []
    fieldA = "hs"
    fieldB = "cnae"
    df = pd.read_csv(file_path, converters={fieldA: str, fieldB: str})
    df = df[ (df[fieldA].str.len() > 0) & (df[fieldB].str.len() >0)]
    df = df[[fieldA, fieldB]]

    if fieldA == "hs":
        df.hs = df.hs.str.slice(2, 6)
        df = df.drop_duplicates()

    print df
    print
    print

    # depths = {"hs" : [2, 6], "cnae": [1, 5]}
    # for depthcol, lengths in depths.items():
    #     my_nesting.append(lengths)
    #     my_nesting_cols.append(depthcol)

    # print my_nesting, my_nesting_cols
    # for depths in itertools.product(*my_nesting):    
    #     series = {}
    #     print depths
    #     for col_name, l in zip(my_nesting_cols, depths):            
    #         series[col_name] = df[col_name].str.slice(0, l)
        
    #     addtl_rows = pd.DataFrame(series)
    #     full_table = pd.concat([addtl_rows, full_table])
    #     # print pk
    # print full_table
    df.to_csv("pi_crosswalk.csv", index=False)

if __name__ == "__main__":
    main()
