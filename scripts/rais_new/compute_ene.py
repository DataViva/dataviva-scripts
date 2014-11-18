''' Import statements '''
import os, sys, time, bz2, click, MySQLdb
import pandas as pd
import numpy as np
from _to_df import to_df

depths = {
    "bra": [1, 3, 5, 7, 8, 9],
    "cnae": [1, 3, 6],
    "cbo": [1, 4]
}

@click.command()
@click.option('output_path', '--output', '-o', help='Path to save files to.', type=click.Path(), required=True, prompt="Output path")
@click.option('--start_from', '-s', help='Path to save files to.')
def main(output_path, start_from):
    start = time.time()
    
    print "Reading rais_df_raw.h5..."
    if not os.path.exists(output_path): os.makedirs(output_path)
    d = pd.HDFStore(os.path.join(output_path, 'rais_df_raw.h5'))
    if "rais_df" in d:
        rais_df = d['rais_df']
    else:
        file_path = raw_input('No rais_df_raw.h5 found, raw file path: ')
        rais_df = to_df(file_path, False)
    d.close()
    
    year = int(rais_df["year"].unique().tolist()[0])
    rais_df = rais_df.drop(["year", "age", "color", "gender", "literacy", "d_id"], axis=1)
    
    cnaes = set(rais_df.cnae_id.unique())
    cnaes = cnaes.union({c[:3] for c in cnaes})
    cnaes = cnaes.union({c[:1] for c in cnaes})
    cnaes = list(cnaes)
    cnaes.sort()
    
    if start_from:
        cnaes = cnaes[cnaes.index(start_from):]
    
    last_seen = {d:{"id":None, "data":None} for d in depths["cnae"][:-1]}
    for i, cnae in enumerate(cnaes):
        s = time.time()
        
        prev_id_len = depths["cnae"].index(len(cnae))-1
        prev_id_len = depths["cnae"][prev_id_len]
        if prev_id_len in last_seen and cnae[:prev_id_len] == last_seen[prev_id_len]["id"]:
            cnae_df = last_seen[prev_id_len]["data"][last_seen[prev_id_len]["data"].cnae_id.str.startswith(cnae)]
        else:
            cnae_df = rais_df[rais_df.cnae_id.str.startswith(cnae)]
        
        if len(cnae) in last_seen:
            last_seen[len(cnae)]["id"] = cnae
            last_seen[len(cnae)]["data"] = cnae_df.copy()
        
        cnae_df = cnae_df[cnae_df.cnae_id.str.startswith(cnae)]
        if len(cnae) < 6:
            cnae_df["cnae_id"] = cnae_df["cnae_id"].str.slice(0, len(cnae))
        
        cnae_df = cnae_df[cnae_df["cnae_id"]==cnae]
        
        # cnae_df['est_id'] = cnae_df['est_id'].str.cat(cnae_df['est_size'].values.astype(str), sep='_')
        # cnae_df = cnae_df.drop(["est_size"], axis=1)
        
        ests = cnae_df.groupby(["est_size", "est_id"]).agg({"wage":pd.Series.median})
        bounds_lower = ests.groupby(level=["est_size"]).agg({"wage":lambda x: x.quantile(0.25)})
        bounds_upper = ests.groupby(level=["est_size"]).agg({"wage":lambda x: x.quantile(0.75)})
        ests = ests.reset_index(level=["est_id"])
        ests["lower"] = bounds_lower["wage"]
        ests["upper"] = bounds_upper["wage"]
        # print ests.shape
        ests = ests[(ests["wage"]>=ests["lower"]) & (ests["wage"]<=ests["upper"])]
        # print ests.shape
        # print ests["est_id"].count(), ests["est_id"].nunique()

        # cnae_df["est_qualify"] = cnae_df["est_id"].apply(lambda x: x in ests["est_id"].tolist())
        # cnae_df = cnae_df[cnae_df["est_qualify"]]
        cnae_df = cnae_df[cnae_df["est_id"].isin(ests["est_id"])]
        
        if cnae_df.empty: continue
        
        num_ests = ests.groupby(level=0).agg({"est_id":pd.Series.count})
        
        # cbos = cnae_df.groupby(["est_size", "cbo_id"]).agg({"est_id":lambda x: set.union(set(x)), "num_emp":pd.Series.count, "wage":pd.Series.median})
        cbos = cnae_df.groupby(["est_size", "cbo_id"]).agg({"est_id": pd.Series.nunique, "num_emp":pd.Series.count, "wage":pd.Series.median})
        cbos = cbos.reset_index(level=["cbo_id"])
        # cbos['test'] = cbos.est_id.str.len()
        cbos['num_est'] = num_ests["est_id"]
        cbos['qualify'] = cbos["est_id"] / cbos["num_est"]
        
        cbos = cbos[cbos['qualify'] >= 0.2]
        cbos["ene"] = cbos["num_emp"] / cbos["est_id"]
        cbos = cbos.reset_index().set_index(["cbo_id", "est_size"])
        # print cbos["ene"].head()
        
        ene = cbos["ene"].unstack(level=-1)
        ene = ene.rename(columns={0:'ene_micro', 1:'ene_small', 2:'ene_medium', 3:'ene_large'})
        ene["cnae_id"] = cnae
        ene["year"] = year
        
        ene = ene.set_index(["year", "cnae_id"], append=True)
        ene = ene.reorder_levels(["year", "cnae_id", "cbo_id"])
        
        print cnae, (time.time() - s) / 60
        
        fname_yio = os.path.join(output_path, 'ene.csv')
        if i == 0 and not start_from:
            ene.to_csv(fname_yio)
        else:
            ene.to_csv(open(fname_yio, 'a'), header=False)
        
    
    print "Done! Merging..."
    
    index_col = ["year", "cnae_id", "cbo_id"]
    full_tbl = pd.read_csv(os.path.join(output_path, "yio.tsv.bz2"), sep="\t", compression="bz2", converters={"cbo_id":str, "cnae_id":str, "year":int})
    full_tbl = full_tbl.set_index(index_col)
    ene_tbl = pd.read_csv(os.path.join(output_path, "ene.csv"), converters={"cbo_id":str, "cnae_id":str, "year":int})
    ene_tbl = ene_tbl.set_index(index_col)
    
    full_tbl = full_tbl.join(ene_tbl, how='outer')
    
    new_file_path = os.path.abspath(os.path.join(output_path, "yio_ene.tsv.bz2"))
    full_tbl.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=True, float_format="%.2f")
    
    print("--- %s minutes ---" % str((time.time() - start)/60))

if __name__ == "__main__":
    main()