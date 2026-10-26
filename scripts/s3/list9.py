import sys
from os.path import join

import pandas as pd
from rbx_s3 import Rbx_client, Rbx_resource


def get_Tagset(s3_client, bucket_name, key):
    response = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)

    tagset = {}
    for tag in response["TagSet"]:
        if tag["Key"] == "uuid":
            tagset["uuid"] = tag["Value"]
        if tag["Key"] == "checksum_md5":
            tagset["checksum_md5"] = tag["Value"]
    return tagset


s3_resource = Rbx_resource(user="user_rw").s3_resource
s3_client = Rbx_client(user="user_rw").s3_client

bucket_name = "mediatheque-patarch-communicable"


bucket = s3_resource.Bucket(bucket_name)
for prefix in sys.argv[1:] or [  # préfixes en argument : relancer certains seulement
     'AMR/PAR_LAB',
     'MED/MED_AFF',
     'MED/MED_CP',
     'MED/MED_IMA',
     'MED/MED_MON',
     'MED/MED_MS',
     'MED/MED_PAR',
     'MED/MED_PUB',
     'MED/MED_VDM',
     'MUS/MUS_VAI',
     'PAR',
     'MED/MED_PAR'
]:
    prefix_path = prefix.replace("/", "__")
    metadata = []
    i = 0
    j = 0
    for obj in bucket.objects.filter(Prefix=prefix):
        result = {
            "key": obj.key,
            "last_modified": obj.last_modified,
            "size": obj.size,
            "storage_class": obj.storage_class,
        }

        tagset = get_Tagset(s3_client, bucket_name, result["key"])

        if "uuid" in tagset:
            result["uuid"] = tagset["uuid"]
        if "checksum_md5" in tagset:
            result["checksum_md5"] = tagset["checksum_md5"]

        metadata.append(result)
        print(i)
        i += 1
        if i % 10000 == 0:
            j += 1
            metadata_df = pd.DataFrame(
                metadata,
                columns=[
                    "key",
                    "last_modified",
                    "size",
                    "storage_class",
                    "uuid",
                    "checksum_md5",
                ],
            )
            metadata_df.to_csv(
                join(
                    "data",
                    "s3",
                    f"listing9-mediatheque-patarch-communicable_{prefix_path}_{j}.csv.gz",
                ),
                index=False,
            )
            metadata = []
    j += 1
    metadata_df = pd.DataFrame(
        metadata,
        columns=[
            "key",
            "last_modified",
            "size",
            "storage_class",
            "uuid",
            "checksum_md5"
        ],
    )
    file_out = join(
        "data",
        "s3",
        f"listing9-mediatheque-patarch-communicable_{prefix_path}_{j}.csv.gz"
    )
    metadata_df.to_csv(file_out, index=False)
