import pandas as pd
from ..helpers import get_file

def pci_wld_eci(eci_file_path, pci_file_path, ymp, ymw):
    
    pci_file = get_file(pci_file_path)
    pcis = pd.read_csv(pci_file, sep=",", converters={"hs_id": str})
    pcis["month"] = "00"
    pcis = pcis.set_index(["year", "month", "hs_id"])
   
    eci_file = get_file(eci_file_path)
    ecis = pd.read_csv(eci_file, sep=",")
    ecis["month"] = "00"
    ecis = ecis.set_index(["year", "month", "wld_id"])
    
    ymp["pci"] = pcis["pci"]
    ymw["eci"] = ecis["eci"]
    
    return [ymp, ymw]
    