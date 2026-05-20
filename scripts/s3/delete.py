from rbx_s3 import Rbx_resource
import pandas as pd

USER = 'user_rw'
bucket = 'mediatheque-patarch-communicable'
rbx_resource = Rbx_resource(user=USER)

objects2del = pd.read_csv("../data-bnr-data-transferts/corr.csv")

for key in objects2del['key'].sort_values():
    print(key)
    rbx_resource.delete_object(bucket, key)
