from os.path import join

import pandas as pd

ref = pd.read_csv("../../_ref/_ref_files_20251226.csv.gz")
az = pd.read_csv(join("data", "bnr_azrael_20260313.csv.gz"))
