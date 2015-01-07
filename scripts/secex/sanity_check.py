''' Import statements '''
import os, sys, time, bz2, click, unittest, logging
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal, assert_series_equal

'''
    Run w/ the following:
    python scripts/secex_new/sanity_check.py  -f data/secex/2000/ -t ympw
'''

def get_tbl(tbl, file_path):
    index_lookup = {"y":"year", "m":"month", "b":"bra_id", "p":"hs_id", "w":"wld_id"}
    file_path = os.path.join(file_path, tbl+".tsv.bz2")
    input_file = bz2.BZ2File(file_path)
    index_cols = [index_lookup[i] for i in tbl]
    df = pd.read_csv(input_file, sep="\t", converters={"month":str, "hs_id":str})
    df = df.set_index(index_cols)
    return index_cols, df

class SecexTests(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self.index_cols, self.tbl = get_tbl(TABLE, FILE_PATH)
        if TABLE == "ymbp":
            self.complexity_tbl = get_tbl("ymp", FILE_PATH)[1]
    
    def test_col_year(self):
        test_tbl = self.tbl.reset_index()
        return self.assertEqual(test_tbl.year.nunique(), 1)

    def test_col_month(self):
        test_tbl = self.tbl.reset_index()
        months = list(test_tbl.month.value_counts().index)
        months = [int(m) for m in months]
        expected_months = range(0, 13)
        return self.assertItemsEqual(months, expected_months)

    def test_col_bra_id(self):
        test_tbl = self.tbl.reset_index()
        if "bra_id" not in test_tbl.columns or "bra_id" not in self.index_cols:
            self.skipTest('no bra_id column')
        states = test_tbl["bra_id"][test_tbl["bra_id"].str.len()==2].unique()
        munics = test_tbl["bra_id"][test_tbl["bra_id"].str.len()==8].unique()
        munics = [m[:2] for m in munics]
        return self.assertItemsEqual(states, set(munics))

    def test_col_bra_id_len(self):
        test_tbl = self.tbl.reset_index()
        if "bra_id_len" not in test_tbl.columns or "bra_id" not in self.index_cols:
            self.skipTest('no bra_id column')
        test_tbl["calculated_len"] = test_tbl["bra_id"].str.len()
        return assert_series_equal(test_tbl["calculated_len"], test_tbl["bra_id_len"])

    def test_col_hs_id(self):
        test_tbl = self.tbl.reset_index()
        if "hs_id" not in test_tbl.columns or "hs_id" not in self.index_cols:
            self.skipTest('no hs_id column')
        hs2 = test_tbl["hs_id"][test_tbl["hs_id"].str.len()==2].unique()
        hs4 = test_tbl["hs_id"][test_tbl["hs_id"].str.len()==6].unique()
        hs4 = [p[:2] for p in hs4]
        return self.assertItemsEqual(hs2, set(hs4))

    def test_col_hs_id_len(self):
        test_tbl = self.tbl.reset_index()
        if "hs_id_len" not in test_tbl.columns or "hs_id" not in self.index_cols:
            self.skipTest('no hs_id column')
        test_tbl["calculated_len"] = test_tbl["hs_id"].str.len()
        return assert_series_equal(test_tbl["calculated_len"], test_tbl["hs_id_len"])

    def test_col_wld_id(self):
        test_tbl = self.tbl.reset_index()
        if "wld_id" not in test_tbl.columns or "wld_id" not in self.index_cols:
            self.skipTest('no wld_id column')
        continents = test_tbl["wld_id"][test_tbl["wld_id"].str.len()==2].unique()
        countries = test_tbl["wld_id"][test_tbl["wld_id"].str.len()==5].unique()
        countries = [c[:2] for c in countries]
        return self.assertItemsEqual(continents, set(countries))

    def test_col_wld_id_len(self):
        test_tbl = self.tbl.reset_index()
        if "wld_id_len" not in test_tbl.columns or "wld_id" not in self.index_cols:
            self.skipTest('no wld_id column')
        test_tbl["calculated_len"] = test_tbl["wld_id"].str.len()
        return assert_series_equal(test_tbl["calculated_len"], test_tbl["wld_id_len"])

    def test_calc_hs_diversity(self):
        test_tbl = self.tbl
        if "hs_diversity" not in test_tbl.columns:
            self.skipTest('no HS diversity column')
        if TABLE == "ymb":
            diversity_tbl = get_tbl("ymbp", FILE_PATH)[1]
            agg_id = "bra_id"
        elif TABLE == "ymw":
            diversity_tbl = get_tbl("ympw", FILE_PATH)[1]
            agg_id = "wld_id"
        diversity_df = test_tbl.reindex(index=["00"], level="month")
        
        expected_df = diversity_tbl.reindex(index=["00"], level="month")
        expected_df = expected_df.dropna(how="any", subset=["export_val"])
        expected_df = expected_df.reset_index(level="hs_id")
        expected_df = expected_df[expected_df["hs_id"].str.len()==6]
        expected_df = expected_df[expected_df["export_val"]>0]
        expected_df = expected_df.groupby(level=["year", "month", agg_id]).agg({"hs_id":pd.Series.count})
        expected_df = expected_df["hs_id"]
        diversity_df = diversity_df["hs_diversity"][diversity_df["hs_diversity"]>0].astype(int)
        return assert_series_equal(diversity_df, expected_df)

    def test_calc_wld_diversity(self):
        test_tbl = self.tbl
        if "wld_diversity" not in test_tbl.columns:
            self.skipTest('no WLD diversity column')
        if TABLE == "ymb":
            diversity_tbl = get_tbl("ymbw", FILE_PATH)[1]
            agg_id = "bra_id"
        elif TABLE == "ymp":
            diversity_tbl = get_tbl("ympw", FILE_PATH)[1]
            agg_id = "hs_id"
        diversity_df = test_tbl.reindex(index=["00"], level="month")
        
        expected_df = diversity_tbl.reindex(index=["00"], level="month")
        expected_df = expected_df.dropna(how="any", subset=["export_val"])
        expected_df = expected_df.reset_index(level="wld_id")
        expected_df = expected_df[expected_df["wld_id"].str.len()==5]
        expected_df = expected_df[expected_df["export_val"]>0]
        expected_df = expected_df.groupby(level=["year", "month", agg_id]).agg({"wld_id":pd.Series.count})
        expected_df = expected_df["wld_id"].astype(int)
        diversity_df = diversity_df["wld_diversity"][diversity_df["wld_diversity"]>0].astype(int)
        return assert_series_equal(diversity_df, expected_df)

    def test_calc_bra_diversity(self):
        test_tbl = self.tbl
        if "bra_diversity" not in test_tbl.columns:
            self.skipTest('no BRA diversity column')
        if TABLE == "ymw":
            diversity_tbl = get_tbl("ymbw", FILE_PATH)[1]
            agg_id = "wld_id"
        elif TABLE == "ymp":
            diversity_tbl = get_tbl("ymbp", FILE_PATH)[1]
            agg_id = "hs_id"
        diversity_df = test_tbl.reindex(index=["00"], level="month")
        
        expected_df = diversity_tbl.reindex(index=["00"], level="month")
        expected_df = expected_df.dropna(how="any", subset=["export_val"])
        expected_df = expected_df.reset_index(level="bra_id")
        expected_df = expected_df[expected_df["bra_id"].str.len()==8]
        expected_df = expected_df[expected_df["export_val"]>0]
        expected_df = expected_df.groupby(level=["year", "month", agg_id]).agg({"bra_id":pd.Series.count})
        expected_df = expected_df["bra_id"].astype(int)
        diversity_df = diversity_df["bra_diversity"][diversity_df["bra_diversity"]>0].astype(int)
        return assert_series_equal(diversity_df, expected_df)

    '''To test the domestic ECI values we are checking to make sure all the bra_ids
         with product exports of any of the HS products with PCI values have ECIs.
         ECIs for a location are calculated by averaging the PCIs of all the products
         exported w/ RCA > 1 in that location.'''
    def test_calc_domestic_eci(self):
        test_tbl = self.tbl.reset_index()
        if "eci" not in test_tbl.columns or TABLE != "ymb":
            self.skipTest('no ECI columns')
        
        ymp = get_tbl("ymp", FILE_PATH)[1]
        ymp = ymp[ymp["pci"].notnull()]
        hs_w_pci = list(ymp.reset_index()["hs_id"])
        
        ymbp = get_tbl("ymbp", FILE_PATH)[1]
        ymbp = ymbp[ymbp["rca"]>=1]
        ymbp = ymbp.reset_index()
        ymbp = ymbp[["bra_id", "hs_id"]]
        ymbp = ymbp[ymbp["hs_id"].isin(hs_w_pci)]
        expected_bras = pd.Series(ymbp["bra_id"].unique())
        
        bras_w_eci = test_tbl[test_tbl["eci"].notnull()]
        bras_w_eci = bras_w_eci["bra_id"].reset_index()["bra_id"]
        
        return assert_series_equal(bras_w_eci, expected_bras)
    
    '''The point of this test is to make sure RCA is present IF export val is
        present for the given row. It does not test for correctness.'''
    def test_calc_rca(self):
        test_tbl = self.tbl.reset_index()
        if "rca" not in test_tbl.columns:
            self.skipTest('no RCA columns')
        # only care for year values NOT month
        month_criterion = test_tbl["month"]=="00"
        # only care if HS is deepest level
        hs_criterion = test_tbl["hs_id"].str.len()==6
        # only care if export_val is not null
        export_val_criterion = test_tbl["export_val"]
        expected_df = test_tbl[month_criterion & hs_criterion & export_val_criterion]
        # only the columns with RCA
        rca_df = test_tbl.dropna(how="any", subset=["rca"])
        return assert_frame_equal(expected_df, rca_df, check_names=True)

    '''The point of this test is to make sure RCD is present IF import val is
        present for the given row. It does not test for correctness.'''
    def test_calc_rcd(self):
        test_tbl = self.tbl.reset_index()
        if "rcd" not in test_tbl.columns:
            self.skipTest('no RCD column')
        # only care for year values NOT month
        month_criterion = test_tbl["month"]=="00"
        # only care if HS is deepest level
        hs_criterion = test_tbl["hs_id"].str.len()==6
        # only care if import_val is not null
        import_val_criterion = test_tbl["import_val"]
        expected_df = test_tbl[month_criterion & hs_criterion & import_val_criterion]
        # only the columns with RCA
        rcd_df = test_tbl.dropna(how="any", subset=["rcd"])
        return assert_frame_equal(expected_df, rcd_df, check_names=True)

    '''The point of this test is to make sure domestic distance values are present
        IF month == 0 (year values) and hs_id length == 6 (full HS product) and
        the given hs_id shows up with export value at least once for that year.
        It does not test for correctness.'''
    def test_calc_distance(self):
        # test_tbl = self.tbl.reset_index()
        test_tbl = self.tbl
        if "rcd" not in test_tbl.columns:
            self.skipTest('no distance column')
        # find hs_ids with export data
        hs_w_export = test_tbl.groupby(level=["hs_id"]).agg({"export_val":np.sum}).dropna()
        hs_w_export = hs_w_export.index
        hs_w_export = [hs for hs in hs_w_export if len(hs) == 6]
        expected_df = test_tbl.reindex(index=hs_w_export, level="hs_id")
        # only care for year values NOT month
        expected_df = expected_df.reindex(index=["00"], level="month")
        # only the columns with distance
        distance_df = test_tbl.dropna(how="any", subset=["distance"])
        return assert_frame_equal(expected_df, distance_df, check_names=True)

    '''Testing for opp gain (need ymp table to find out which hs_ids have PCI).'''
    def test_calc_opp_gain(self):
        test_tbl = self.tbl
        if "opp_gain_wld" not in test_tbl.columns:
            self.skipTest('no world opportunity gain column')
        pci_tbl = self.complexity_tbl
        # find non planning region bra_ids
        bras_non_plr = test_tbl.index.get_level_values('bra_id')
        bras_non_plr = [bra for bra in bras_non_plr if len(bra) != 7]
        expected_df = test_tbl.reindex(index=bras_non_plr, level="bra_id")
        # find hs_ids with PCI data
        hs_w_pci = pci_tbl.groupby(level=["hs_id"]).agg({"pci":np.sum}).dropna()
        hs_w_pci = hs_w_pci.index
        expected_df = expected_df.reindex(index=hs_w_pci, level="hs_id")
        # only care for year values NOT month
        expected_df = expected_df.reindex(index=["00"], level="month")
        # only the columns with opp gain
        opp_gain_wld_df = test_tbl.dropna(how="any", subset=["opp_gain", "opp_gain_wld"])
        return assert_frame_equal(expected_df, opp_gain_wld_df, check_names=True)

    def test_months_year(self):
        # only testing export value for now
        test_tbl = self.tbl[["export_val"]]
        test_tbl = test_tbl.reset_index()
        # get index columns minus the month!
        cols = self.index_cols
        del cols[cols.index("month")]
        # get total for entire year without year total
        tbl_wo_year_total = test_tbl[test_tbl['month']!="00"]
        tbl_wo_year_total = tbl_wo_year_total.groupby(cols).sum()
        # create dataframe with only year totals
        tbl_year_total = test_tbl[test_tbl['month']=="00"]
        tbl_year_total = tbl_year_total.set_index(cols)
        tbl_year_total = tbl_year_total.drop("month", axis=1)
        # these 2 dataframes should be equal... the table with out years and the year totals
        tbl_wo_year_total = tbl_wo_year_total.dropna()
        tbl_year_total = tbl_year_total.dropna()
        return assert_frame_equal(tbl_wo_year_total, tbl_year_total, check_names=True)

    def test_null_cols(self):
        test_tbl = self.tbl.reset_index()
        ''' USE THIS TO MAKE TEST FAIL:
            test_tbl['asdf'] = np.nan
        '''
        cols = test_tbl.dtypes[(test_tbl.dtypes==int)|(test_tbl.dtypes==float)]
        log = logging.getLogger( "SecexTests.test_null_cols" )
        for c in cols.index:
            if test_tbl[c].sum() == 0 or np.isnan(test_tbl[c].sum()):
                # log.debug("[ERROR] column = %r", c)
                self.fail("error in '{0}' column".format(c))
        return True

@click.command()
@click.option('file_path', '--files', '-f', help='Path of saved files.', type=click.Path(), required=True, prompt="Path")
@click.option('table', '--table', '-t', help='Table to test.', required=True, prompt="Table")
def main(file_path, table):
    global FILE_PATH, TABLE
    FILE_PATH, TABLE = [file_path, table]
    del sys.argv[1:]
    
    logging.basicConfig( stream=sys.stderr )
    logging.getLogger( "SecexTests.test_null_cols" ).setLevel( logging.DEBUG )
    
    unittest.main()

if __name__ == '__main__':
    main()