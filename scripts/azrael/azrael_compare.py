from os.path import join

import pandas as pd

df1 = pd.read_csv(join("results", "ref", "_ref_files_20260411.csv.gz"))
df2 = pd.read_csv(join("data", "az", "bnr_azrael_20260502.csv.gz"))

df1 = df1[["name", "path", "size", "checksum_md5", "uuid"]]

df2 = df2.merge(df1, on=["name", "path", "size"], how="left")

df2_inconnu = df2[df2["uuid"].isna()]
df2_inconnu.to_csv(join("data", "az", "bnr_azrael_20260502_nouuid.csv.gz"), index=False)
