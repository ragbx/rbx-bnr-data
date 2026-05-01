import pandas as pd
from os.path import join
from os import listdir
from datetime import datetime

date = datetime.now().strftime("%Y%m%d")

ifiles = [f for f in listdir(join("data", "oai", "sets_identifiers")) if ".csv" in f]
ifiles_df = []
for f in ifiles:
    df_ = pd.read_csv(join("data", "oai", "sets_identifiers", f))
    ifiles_df.append(df_)
i_df = pd.concat(ifiles_df)

rfiles = [f for f in listdir(join("data", "oai", "sets_records")) if ".csv" in f]
rfiles_df = []
for f in rfiles:
    df_ = pd.read_csv(join("data", "oai", "sets_records", f))
    rfiles_df.append(df_)
r_df = pd.concat(rfiles_df)

df = r_df.merge(i_df, on="identifier", how='outer')
df.to_csv(join("data", "oai", f"oai_records_{date}.csv.gz"), index=False)
