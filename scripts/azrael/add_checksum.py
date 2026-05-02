from os.path import join

import pandas as pd
from bnr.azrael import Azrael2list

date = "20260502"

df = pd.read_csv(join("data", "az", f"bnr_azrael_{date}_nouuid.csv.gz"))

# root_path = '\\\\ntrbx.local\mediatheque\BNR'
root_path = "../bnr"
print(root_path)
az2list = Azrael2list(root_path=root_path, code_disk=None, az=df)
az2list.get_all_checksum(new_checksum_file_name="new_checksum")
az2list.save_list(filename=join("data", "az", "bnr_azrael_{date}_nouuid-cs.csv.gz"))
