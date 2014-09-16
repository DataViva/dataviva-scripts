import pandas as pd

def pci_wld_eci(eci_file_path, pci_file_path, ymp, ymw):
    
    pcis = pd.read_csv(pci_file_path, sep=",", converters={"hs_id": str})
    pcis["month"] = "00"
    pcis = pcis.set_index(["year", "month", "hs_id"])
   
    ecis = pd.read_csv(eci_file_path, sep=",")
    ecis["month"] = "00"
    ecis = ecis.set_index(["year", "month", "wld_id"])
    
    ymp["pci"] = pcis["pci"]
    ymw["eci"] = ecis["eci"]
    
    return [ymp, ymw]
    