import pandas as pd
from os.path import join

df1 = pd.read_csv(join("data", "_ref_files_20251226.csv.gz"))
df2 = pd.read_csv(join("data", "bnr_azrael_20260411.csv.gz"))

df1 = df1[['name', 'path', 'size', 'checksum_md5', 'uuid']]

df2 = df2.merge(df1, on=['name', 'path', 'size'], how='left')

df2_inconnu = df2[df2['uuid'].isna()]
df2_inconnu.to_csv(join("data", "bnr_azrael_20260411_nouuid.csv.gz"), index=False)
