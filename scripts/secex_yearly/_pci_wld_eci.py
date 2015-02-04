import pandas as pd

def pci_wld_eci(eci_file_path, pci_file_path, yp, yw):
    
    pcis = pd.read_csv(pci_file_path, sep=",", converters={"hs_id": str})
    pcis = pcis.set_index(["year", "hs_id"])
   
    ecis = pd.read_csv(eci_file_path, sep=",")
    ecis = ecis.set_index(["year", "wld_id"])
    
    yp["pci"] = pcis["pci"]
    yw["eci"] = ecis["eci"]
    
    return [yp, yw]
    