from os.path import join

import pandas as pd

df1 = pd.read_csv(join("results", "ref", "_ref_files_20260411.csv.gz"))
df2 = pd.read_csv(join("data", "az", "bnr_azrael_20260502.csv.gz"))
df2 = df2[df2["name"].str[-3:] != ".db"]

df1["m"] = df1["last_content_modification_date"]
df1["c"] = df1["last_metadata_modification_date"]
df1["last_metadata_modification_date"] = df1["m"]
df1["last_content_modification_date"] = df1["c"]

df1 = df1[
    [
        "name",
        "path",
        "size",
        "last_content_modification_date",
        "last_metadata_modification_date",
        "checksum_md5",
        "uuid",
    ]
]

df3 = df2.merge(
    df1,
    on=[
        "name",
        "path",
        "size",
        "last_content_modification_date",
        "last_metadata_modification_date",
    ],
    how="left",
)

df3_inconnu = df3[df3["uuid"].isna()]
df3_inconnu.to_csv(join("data", "az", "bnr_azrael_20260502_nouuid.csv.gz"), index=False)
