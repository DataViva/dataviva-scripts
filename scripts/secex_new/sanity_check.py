# -*- coding: utf-8 -*-
''' Import statements '''
import os, sys, time, bz2, click, unittest, logging
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal

'''
    Run w/ the following:
    python scripts/secex_new/sanity_check/sanity_check.py  -f data/secex/2000/ -t ympw
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
        if "bra_id" not in test_tbl.columns and "bra_id" not in self.index_cols:
            self.skipTest('no bra_id column')
        states = test_tbl["bra_id"][test_tbl["bra_id"].str.len()==2].unique()
        munics = test_tbl["bra_id"][test_tbl["bra_id"].str.len()==8].unique()
        munics = [m[:2] for m in munics]
        return self.assertItemsEqual(states, set(munics))
    
    def test_col_hs_id(self):
        test_tbl = self.tbl.reset_index()
        if "hs_id" not in test_tbl.columns and "hs_id" not in self.index_cols:
            self.skipTest('no hs_id column')
        hs2 = test_tbl["hs_id"][test_tbl["hs_id"].str.len()==2].unique()
        hs4 = test_tbl["hs_id"][test_tbl["hs_id"].str.len()==6].unique()
        hs4 = [p[:2] for p in hs4]
        return self.assertItemsEqual(hs2, set(hs4))
        
    def test_col_wld_id(self):
        test_tbl = self.tbl.reset_index()
        if "wld_id" not in test_tbl.columns and "wld_id" not in self.index_cols:
            self.skipTest('no wld_id column')
        continents = test_tbl["wld_id"][test_tbl["wld_id"].str.len()==2].unique()
        countries = test_tbl["wld_id"][test_tbl["wld_id"].str.len()==5].unique()
        countries = [c[:2] for c in countries]
        return self.assertItemsEqual(continents, set(countries))
    
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