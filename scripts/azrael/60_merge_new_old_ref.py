import mimetypes
from os.path import join

import pandas as pd

from _pipeline import NEW_REF_DATE, OLD_REF_DATE, REF_DIR, ref_file


def get_mimetype(filename, fillna="N/A"):
    if filename:
        mimetype, _ = mimetypes.guess_type(filename)
        if fillna:
            if mimetype == None:
                mimetype = fillna
        return mimetype


old_ref_date = OLD_REF_DATE
new_ref_date = NEW_REF_DATE

ref = pd.read_csv(ref_file(old_ref_date))
# interface externe : produit par le maillon d'enrichissement s3/dao/oai (hors dépôt)
tmp = pd.read_csv(
    join(REF_DIR, f"_ref_files_{new_ref_date}_tmp_s3_dao_oai.csv.gz")
)
tmp = tmp[~tmp["name"].isna()]

m1 = tmp.merge(ref, on=["uuid", "checksum_md5"], how="left")

nc = ["uuid", "checksum_md5"]
nc.extend(list(set(ref.columns) ^ set(tmp.columns)))
new_columns = []
for c in m1.columns:
    if c in nc:
        new_columns.append(c)
    elif c[-2:] == "_x":
        if c == "mix_xSamplingFrequency_x":
            new_columns.append("mix_xSamplingFrequency_new")
        else:
            new_columns.append(c.replace("_x", "_new"))
    elif c[-2:] == "_y":
        if c == "mix_ySamplingFrequency_y":
            new_columns.append("mix_ySamplingFrequency_old")
        else:
            new_columns.append(c.replace("_y", "_old"))

m1.columns = new_columns

"""
ref.columns = ['name', 'path', 'size', 'last_content_modification_date',
       'last_metadata_modification_date', 'checksum_md5', 'uuid', 'extension',
       'file_type', 'source2s3', 'conservation_statut', 'finding_aid',
       'unitid', 'osiros_id', 'mix_objectIdentifierValue', 'mix_fileSize',
       'mix_dateTimeCreated', 'mix_formatName', 'mix_byteOrder',
       'mix_compressionScheme', 'mix_imageWidth', 'mix_imageHeight',
       'mix_xSamplingFrequency', 'mix_ySamplingFrequency',
       'mix_samplingFrequencyUnit', 'mix_colorSpace',
       'mix_scanningSoftwareName', 'mix_formatVersion', 'publication_statut',
       'corpus_code', 'oai_set', 's3_key', 's3_uploaded', 's3_uploaded_date',
       's3_bucket']
on découpe le dataframe en segments :
    - ce qui relève de az : 'name', 'path', 'size', 'last_content_modification_date',
           'last_metadata_modification_date', 'checksum_md5', 'uuid', 'extension',
           'file_type', 'source2s3',
    - ce qui relève de s3 : 's3_key', 's3_uploaded', 's3_uploaded_date', 's3_bucket'
    - ce qui relève de oai et dao : 'oai_set', 'finding_aid', 'unitid', 'osiros_id'
    - ce qui relève de mix : 'mix_objectIdentifierValue', 'mix_fileSize', 'mix_dateTimeCreated',
    'mix_formatName', 'mix_byteOrder', 'mix_compressionScheme', 'mix_imageWidth', 'mix_imageHeight',    'mix_xSamplingFrequency', 'mix_ySamplingFrequency',
    'mix_samplingFrequencyUnit', 'mix_colorSpace', 'mix_scanningSoftwareName', 'mix_formatVersion'
    - ce qui relève d'information sur le traitement des fichiers : 'conservation_statut', 'corpus_code', 'publication_statut'
"""

# ce qui relève de az : attention à uuid, extension, file_type, source2s3
len(m1[m1["uuid"].isna()])

len(m1[m1["extension_new"].isna()])
m1.loc[m1["extension_new"].isna(), "extension_new"] = m1["name_new"].str.extract(
    r".*(\..*)$"
)[0]

m1["mimetype_new"] = m1["name_new"].apply(get_mimetype)
m1["file_type_new"] = m1["mimetype_new"]
m1.loc[m1["file_type_new"] == "image/jpeg", "file_type_new"] = "jpeg"
m1.loc[m1["extension_new"] == ".jp2", "file_type_new"] = "jpeg2000"
m1.loc[m1["file_type_new"] == "image/tiff", "file_type_new"] = "tiff"
m1.loc[m1["file_type_new"] == "text/plain", "file_type_new"] = "ocr txt"
m1.loc[m1["file_type_new"] == "application/pdf", "file_type_new"] = "pdf"
m1.loc[m1["file_type_new"] == "application/xml", "file_type_new"] = "ocr xml"
m1.loc[m1["extension_new"] == ".alto", "file_type_new"] = "ocr xml"
m1.loc[m1["file_type_new"].str[:4] == "vide", "file_type_new"] = "video"
m1.loc[m1["extension_new"] == ".MTS", "file_type_new"] = "video"
m1.loc[m1["file_type_new"].str[:4] == "audi", "file_type_new"] = "audio"
m1.loc[
    ~m1["file_type_new"].isin(
        [
            "jpeg",
            "jpeg2000",
            "tiff",
            "pdf",
            "ocr txt",
            "ocr xml",
            "video",
            "audio",
        ]
    ),
    "file_type_new",
] = "autre"

len(m1[m1["source2s3_new"].isna()])
m1.loc[m1["source2s3_new"].isna(), "source2s3_new"] = "az"

# bloc s3
# 's3_key', 's3_uploaded', 's3_uploaded_date', 's3_bucket'
m1.loc[~m1["s3_key_"].isna(), "s3_key_new"] = m1["s3_key_"]
m1.loc[~m1["s3_uploaded_date_"].isna(), "s3_uploaded_date_new"] = m1[
    "s3_uploaded_date_"
]
m1["s3_uploaded_new"] = False
m1.loc[~m1["s3_uploaded_date_"].isna(), "s3_uploaded_new"] = True
m1["s3_bucket_new"] = None
m1.loc[m1["s3_uploaded_new"], "s3_bucket_new"] = "mediatheque-patarch_communicable"
m1.loc[
    (~m1["s3_key_new"].isna()) & (m1["s3_key_new"].str.contains("AMR_EC")),
    "s3_bucket_new",
] = "mediatheque-patarch_incommunicable"

# bloc oai et dao
# 'oai_set', 'finding_aid', 'unitid', 'osiros_id'
m1.loc[~m1["oai_setname"].isna(), "oai_set_new"] = m1["oai_setname"]
m1.loc[(m1["oai_set_new"].isna()) & (~m1["oai_set_old"].isna()), "oai_set_new"] = m1[
    "oai_set_old"
]

m1.loc[~m1["dao_finding_aid"].isna(), "finding_aid_new"] = m1["dao_finding_aid"]
m1.loc[
    (m1["finding_aid_new"].isna()) & (~m1["finding_aid_old"].isna()), "finding_aid_new"
] = m1["finding_aid_old"]

m1.loc[~m1["dao_unitid"].isna(), "unitid_new"] = m1["dao_unitid"]
m1.loc[(m1["unitid_new"].isna()) & (~m1["unitid_old"].isna()), "unitid_new"] = m1[
    "unitid_old"
]

m1.loc[~m1["oai_osiros_id"].isna(), "osiros_id_new"] = m1["oai_osiros_id"]
m1.loc[
    (m1["osiros_id_new"].isna()) & (~m1["osiros_id_old"].isna()), "osiros_id_new"
] = m1["osiros_id_old"]

# bloc mix
for c in [
    "mix_objectIdentifierValue",
    "mix_fileSize",
    "mix_dateTimeCreated",
    "mix_formatName",
    "mix_byteOrder",
    "mix_compressionScheme",
    "mix_imageWidth",
    "mix_imageHeight",
    "mix_xSamplingFrequency",
    "mix_ySamplingFrequency",
    "mix_samplingFrequencyUnit",
    "mix_colorSpace",
    "mix_scanningSoftwareName",
    "mix_formatVersion",
]:
    c_new = c + "_new"
    c_old = c + "_old"
    m1[c_new] = m1[c_old]

# bloc traitement des fichiers
# 'conservation_statut', 'corpus_code', 'publication_statut'
m1.loc[~m1["conservation_statut_old"].isna(), "conservation_statut_new"] = m1[
    "conservation_statut_old"
]
m1.loc[~m1["corpus_code_old"].isna(), "corpus_code_new"] = m1["corpus_code_old"]
m1.loc[~m1["publication_statut_old"].isna(), "publication_statut_new"] = m1[
    "publication_statut_old"
]

# on met de côté les fichiers dans ref mais pas m1
m2 = ref[~ref["uuid"].isin(m1["uuid"])]

# on vérifie l'intégrité des différentes colonnes

new_names = []
for c in m1.columns:
    if c[-4:] == "_new":
        c = c[:-4]
        new_names.append(c)
    else:
        new_names.append(c)
m1.columns = new_names
m1 = m1[ref.columns]

m1.to_csv(ref_file(new_ref_date), index=False)
