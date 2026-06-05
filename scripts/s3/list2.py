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

for prefix in ["CSV", "LAI", "LAR", "MDF", "MUS", "OBS", "PAR", "TCG"]:
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
                    f"listing2-mediatheque-patarch-communicable_{prefix}_{j}.csv.gz",
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
            "checksum_md5",
        ],
    )
    metadata_df.to_csv(
        join(
            "data",
            "s3",
            f"listing2-mediatheque-patarch-communicable_{prefix}_{j}.csv.gz",
        ),
        index=False,
    )
