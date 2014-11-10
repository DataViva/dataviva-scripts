''' Import statements '''
import os, sys, time, bz2, click, MySQLdb
import pandas as pd
import pandas.io.sql as sql
import numpy as np

depths = {
    "bra": [1, 3, 5, 7, 8, 9],
    "cnae": [1, 3, 6],
    "cbo": [1, 4]
}

def get_planning_regions():
    ''' Connect to DB '''
    db = MySQLdb.connect(host="localhost", user=os.environ["DATAVIVA2_DB_USER"], 
                            passwd=os.environ["DATAVIVA2_DB_PW"], 
                            db=os.environ["DATAVIVA2_DB_NAME"])
    db.autocommit(1)
    cursor = db.cursor()
    
    cursor.execute("select bra_id, pr_id from attrs_bra_pr")
    return {r[0]:r[1] for r in cursor.fetchall()}

@click.command()
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
@click.option('--start_from', '-s', help='Path to save files to.')
def main(output_path, start_from):
    
    print "Reading rais_df_raw.h5..."
    if not os.path.exists(output_path): os.makedirs(output_path)
    d = pd.HDFStore(os.path.join(output_path, 'rais_df_raw.h5'))
    if "rais_df" in d:
        rais_df = d['rais_df']
    else:
        sys.exit('Need rais_df_raw.h5!')
    d.close()
    
    cnaes = set(rais_df.cnae_id.unique())
    cnaes = cnaes.union({c[:3] for c in cnaes})
    cnaes = cnaes.union({c[:1] for c in cnaes})
    cnaes = list(cnaes)
    cnaes.sort()
    
    if start_from:
        cnaes = cnaes[cnaes.index(start_from):]
    
    for i, cnae in enumerate(cnaes):
        s = time.time()
        
        cnae_df = rais_df.copy()
        if len(cnae) < 6:
            cnae_df["cnae_id"] = cnae_df["cnae_id"].str.slice(0, len(cnae))
        cnae_df = cnae_df[cnae_df["cnae_id"]==cnae]
        
        cbo_depths = depths["cbo"]
        cnae_depths = depths["cnae"]
        geo_depths = depths["bra"]
    
        cnae_df = cnae_df.drop(["wage", "age", "color", "gender", "literacy"], axis=1)
    
        # cnae_df['mne_micro'] = cnae_df.apply(lambda x: x["num_emp"] if x["est_size"]==0 else None, axis=1)
        # cnae_df['mne_small'] = cnae_df.apply(lambda x: x["num_emp"] if x["est_size"]==1 else None, axis=1)
        # cnae_df['mne_medium'] = cnae_df.apply(lambda x: x["num_emp"] if x["est_size"]==2 else None, axis=1)
        # cnae_df['mne_large'] = cnae_df.apply(lambda x: x["num_emp"] if x["est_size"]==3 else None, axis=1)
    
        cnae_df['est_id_size'] = cnae_df.apply(lambda x: "{0}_{1}".format(x["est_id"], x["est_size"]), axis=1)

        cnae_df = cnae_df.drop(["est_id", "est_size"], axis=1)
    
        pk = ["year", "bra_id", "cnae_id", "cbo_id", "est_id_size"]
        cnae_df = cnae_df.groupby(pk).agg({"num_emp" : lambda x: set.union(set(x))})
    
        # print cnae_df.head()
    
        # print "cbo"
        ybio_new_depths = pd.DataFrame()
        for depth in cbo_depths[:-1]:
            # print "  ", depth
            ybio_depth = cnae_df.reset_index()
            ybio_depth["cbo_id"] = ybio_depth["cbo_id"].str.slice(0, depth)
            ybio_depth = ybio_depth.groupby(pk).agg({"num_emp" : lambda x: set.union(*list(x))})
            ybio_new_depths = pd.concat([ybio_new_depths, ybio_depth])
        cnae_df = pd.concat([ybio_new_depths, cnae_df])
    
        # # print "cnae"
        # ybio_new_depths = pd.DataFrame()
        # for depth in cnae_depths[:-1]:
        #     if cnae[:depth] in cnae_seen: continue
        #     # print "  ", depth
        #     ybio_depth = cnae_df.reset_index()
        #     ybio_depth["cnae_id"] = ybio_depth["cnae_id"].str.slice(0, depth)
        #     ybio_depth = ybio_depth.groupby(pk).agg({"num_emp" : lambda x: set.union(*list(x))})
        #     ybio_new_depths = pd.concat([ybio_new_depths, ybio_depth])
        #     cnae_seen.append(cnae[:depth])
        # cnae_df = pd.concat([ybio_new_depths, cnae_df])
    
        # print "bra"
        ybio_new_depths = pd.DataFrame()
        for depth in geo_depths[:-1]:
            # print "  ", depth
            ybio_depth = cnae_df.reset_index()
            if depth == 8:
                ybio_depth = ybio_depth[ybio_depth["bra_id"].map(lambda x: x[:3] == "4mg")]
                ybio_depth["bra_id"] = ybio_depth["bra_id"].astype(str).replace(get_planning_regions())
            else:
                ybio_depth["bra_id"] = ybio_depth["bra_id"].str.slice(0, depth)
            ybio_depth = ybio_depth.groupby(pk).agg({"num_emp" : lambda x: set.union(*list(x))})
            ybio_new_depths = pd.concat([ybio_new_depths, ybio_depth])
        cnae_df = pd.concat([ybio_new_depths, cnae_df])
    
        cnae_df = cnae_df.reset_index()
        cnae_df["est_id_size"] = cnae_df["est_id_size"].apply(lambda x: x.split("_")[1])
        cnae_df["num_emp"] = cnae_df["num_emp"].apply(len)
    
        cnae_df_ybio = cnae_df.groupby(pk).agg({"num_emp" : pd.Series.median})["num_emp"].unstack(level=-1)
        cnae_df_ybio = cnae_df_ybio.rename(columns={'0':'mne_micro', '1':'mne_small', '2':'mne_medium', '3':'mne_large'})
        
        pk = [p for p in pk if p != "bra_id"]
        cnae_df_yio = cnae_df.groupby(pk).agg({"num_emp" : pd.Series.median})["num_emp"].unstack(level=-1)
        cnae_df_yio = cnae_df_yio.rename(columns={'0':'mne_micro', '1':'mne_small', '2':'mne_medium', '3':'mne_large'})
        
        print cnae, (time.time() - s) / 60
        
        fname_ybio = os.path.join(output_path, 'mne_ybio.csv')
        fname_yio = os.path.join(output_path, 'mne_yio.csv')
        if i == 0 and not start_from:
            cnae_df_ybio.to_csv(fname_ybio)
            cnae_df_yio.to_csv(fname_yio)
        else:
            cnae_df_ybio.to_csv(open(fname_ybio, 'a'), header=False)
            cnae_df_yio.to_csv(open(fname_yio, 'a'), header=False)
    
    print "Done! Merging..."
    
    index_lookup = {"y":"year", "b":"bra_id", "i":"cnae_id", "o":"cbo_id"}
    for tbl in ["ybio", "yio"]:
        print tbl
        index_col = [index_lookup[i] for i in tbl]
        full_tbl = pd.read_csv(os.path.join(output_path, "{0}.tsv.bz2".format(tbl)), sep="\t", compression="bz2", converters={"cbo":str, "cnae_id":str, "bra_id":str}, index_col=index_col)
        mne_tbl = pd.read_csv(os.path.join(output_path, "mne_{0}.csv".format(tbl)), converters={"cbo":str, "cnae_id":str, "bra_id":str}, index_col=index_col)
    
        full_tbl["mne_micro"] = mne_tbl["mne_micro"]
        full_tbl["mne_small"] = mne_tbl["mne_small"]
        full_tbl["mne_medium"] =mne_tbl["mne_medium"]
        full_tbl["mne_large"] = mne_tbl["mne_large"]
    
        new_file_path = os.path.abspath(os.path.join(output_path, "{0}_mne.tsv.bz2".format(tbl)))
        full_tbl.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.2f")

if __name__ == "__main__":
    start = time.time()

    main()
    
    total_run_time = (time.time() - start) / 60
    print; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;
