import sys
import numpy as np
def merge(secex_exports, secex_imports, debug=False):
    
    secex_exports = secex_exports.rename(columns={"val_usd":"export_val"})
    secex_exports = secex_exports.groupby(["year", "month", "state", "munic", "hs", "wld"]).sum()
    
    secex_imports = secex_imports.rename(columns={"val_usd":"import_val"})
    secex_imports = secex_imports.groupby(["year", "month", "state", "munic", "hs", "wld"]).sum()
    
    secex_df = secex_exports.merge(secex_imports, how="outer", left_index=True, right_index=True)

    return secex_df
    