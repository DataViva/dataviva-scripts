import sys
import numpy as np
def merge(secex_exports, secex_imports):
    
    secex_exports = secex_exports.rename(columns={"val_usd":"export_val","val_kg":"export_kg","val_unit":"export_unit"})
    secex_exports = secex_exports.drop(["export_kg", "export_unit", "unit"], axis=1)
    secex_exports = secex_exports.groupby(["year", "month", "state_id", "bra_id", "hs_id", "wld_id"]).sum()
    
    secex_imports = secex_imports.rename(columns={"val_usd":"import_val","val_kg":"import_kg","val_unit":"import_unit"})
    secex_imports = secex_imports.drop(["import_kg", "import_unit", "unit"], axis=1)
    secex_imports = secex_imports.groupby(["year", "month", "state_id", "bra_id", "hs_id", "wld_id"]).sum()
    
    secex_df = secex_exports.merge(secex_imports, how="outer", left_index=True, right_index=True)

    return secex_df
    