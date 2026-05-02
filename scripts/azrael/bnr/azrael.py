"""
On a besoin d'effctuer les tâches suivantes :

- sur un répertoire :
    - obtenir une liste de tous les fichiers présents
    - effectuer des traitements à partir de cette liste (calcul MD5, obtention de métadonnées)
voir Azrael2list

-
"""

import concurrent.futures
import gzip
import hashlib
import json
import mimetypes
import re
import uuid
from datetime import datetime
from os import sep, walk
from os.path import getctime, getmtime, getsize, join
from subprocess import run

import numpy as np
import pandas as pd
from exiftool import ExifToolHelper
from lxml import etree


def get_md5hash(name):
    hash_md5 = hashlib.md5()
    with open(name, "rb") as f:
        while chunk := f.read(4096):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_mimetype(filename, fillna="N/A"):
    if filename:
        mimetype, _ = mimetypes.guess_type(filename)
        if fillna:
            if mimetype == None:
                mimetype = fillna
        return mimetype


def get_extension_by_mimetype(mimetype, fillna="N/A"):
    if mimetype:
        guessed_extension = mimetypes.guess_extension(mimetype)
        if fillna:
            if mimetype == None:
                guessed_extension = fillna
        return guessed_extension


def convert_size(size, from_size="o", to_size="go"):
    if (from_size == "o") & (to_size == "ko"):
        n = 1024
        return round(size / n, 2)
    elif (from_size == "o") & (to_size == "mo"):
        n = 1024**2
        return round(size / n, 2)
    elif (from_size == "o") & (to_size == "go"):
        n = 1024**3
        return round(size / n, 2)
    elif (from_size == "o") & (to_size == "to"):
        n = 1024**4
        return round(size / n, 2)


def split_every_n_rows(dataframe, chunk_size=2):
    chunks = []
    num_chunks = len(dataframe) // chunk_size + 1
    for index in range(num_chunks):
        chunks.append(dataframe[index * chunk_size : (index + 1) * chunk_size])
    return chunks


def int2string(n, leading_zeros=8):
    return str(n).zfill(leading_zeros)


def jhove_execution(file_pathes_chunks, chunk_id, jhove_path):
    pathes = " ".join(file_pathes_chunks)
    jhove_res_file = f"data/jhove_chunk_{chunk_id}.xml"
    command = f"./{jhove_path} -h xml {pathes} > {jhove_res_file}"
    run(command, shell=True, executable="/bin/bash")
    command = f"gzip {jhove_res_file}"
    run(command, shell=True, executable="/bin/bash")


class Azrael2list:
    def __init__(self, **kwargs):
        self.today = datetime.now().strftime("%Y%m%d")
        if "code_disk" in kwargs:
            self.code_disk = kwargs.get("code_disk")
        else:
            self.code_disk = None
        if "root_path" in kwargs:
            self.root_path = kwargs.get("root_path")
        if "az" in kwargs:
            self.az = kwargs.get("az")

    def list_files(self, checksum_md5=False):
        list_results = []
        for dir_path, dirs, files in walk(self.root_path):
            print(dir_path)
            for file in files:
                file_path = join(dir_path, file)
                file_data = {}
                file_data["name"] = file
                file_data["path"] = dir_path.replace(self.root_path, "")
                file_data["size"] = getsize(file_path)
                if checksum_md5:
                    file_data["checksum_md5"] = get_md5hash(file_path)
                file_data["last_content_modification_date"] = getmtime(file_path)
                file_data["last_metadata_modification_date"] = getctime(file_path)
                list_results.append(file_data)
        self.az = pd.DataFrame(list_results).sort_values(by=["path", "name"])

    def get_all_checksum(self, new_checksum_file_name="new_checksum"):
        self.az["filename"] = self.az["path"].str.cat(self.az["name"], sep=sep)
        if self.root_path:
            self.az["filename"] = self.root_path + sep + self.az["filename"]
        checksum_ko = self.az[self.az["checksum_md5"].isna()]
        checksum_ok = self.az[~self.az["checksum_md5"].isna()]

        def get_checksum(file_data, results):
            try:
                file_data["checksum_md5"] = get_md5hash(file_data["filename"])
                print(file_data)
                results.append(file_data)
            except:
                pass

        i = 0
        j = 0
        n = 1000

        while i < len(checksum_ko):
            df_data = checksum_ko[i : i + n]
            new_checksum = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for file_data in df_data.to_dict(orient="records"):
                    print(file_data)
                    futures.append(
                        executor.submit(
                            get_checksum, file_data=file_data, results=new_checksum
                        )
                    )
                for future in concurrent.futures.as_completed(futures):
                    future.result()

            j += 1
            new_checksum_df = pd.DataFrame(new_checksum)
            if "filename" in new_checksum_df:
                new_checksum_df = new_checksum_df.drop(columns=["filename"])
            new_checksum_df.to_csv(
                join("data", "az", "tmp", f"{new_checksum_file_name}_{j}.csv.gz"),
                index=False,
            )

            i += n

    def save_list(self, filename=None):
        if filename:
            self.az.to_csv(filename, index=False)
        else:
            if self.code_disk:
                self.az.to_csv(
                    join(
                        "data", "az", f"bnr_azrael_{self.code_disk}_{self.today}.csv.gz"
                    ),
                    index=False,
                )
            else:
                self.az.to_csv(
                    join("data", "az", f"bnr_azrael_{self.today}.csv.gz"), index=False
                )


class Azrael2analysis:
    def __init__(self):
        self.today = datetime.now().strftime("%Y%m%d")

    def create_az(self, **kwargs):
        if "path_prefix" in kwargs:
            self.path_prefix = kwargs.get("path_prefix")
        if "az" in kwargs:
            self.az = kwargs.get("az")
        elif "path_az" in kwargs:
            path_az = kwargs.get("path_az")
            self.az = pd.read_csv(path_az)
        self.az = self.az.sort_values(by=["path", "name"])
        if hasattr(self, "path_prefix"):
            self.az["path"] = self.az["path"].str.replace(self.path_prefix, "")

    def add_bnr_file_id(self, disk=None):
        self.az = self.az.assign(bnr_file_id=range(1, len(self.az) + 1))
        if disk:
            self.az["bnr_file_id"] = self.az["bnr_file_id"].apply(
                lambda x: f"bnr_{disk}_{str(x).zfill(8)}"
            )
        else:
            self.az["bnr_file_id"] = self.az["bnr_file_id"].apply(
                lambda x: f"bnr_{str(x).zfill(8)}"
            )

    def split_path(self, n=4):
        cols = [f"path{n}" for n in range(n + 1)]
        self.az[cols] = self.az["path"].str.split("/", n=n, expand=True)

    def dates2dt(self):
        # traitement des dates création et modification

        # on transforme les colonnes dates en objets datetime
        self.az["last_content_modification_date_dt"] = pd.to_datetime(
            self.az["last_content_modification_date"], unit="s"
        )
        self.az["last_metadata_modification_date_dt"] = pd.to_datetime(
            self.az["last_metadata_modification_date"], unit="s"
        )

        self.az["last_content_modification_date_"] = self.az[
            "last_content_modification_date_dt"
        ].dt.strftime("%Y-%m-%d")
        self.az["last_metadata_modification_date_"] = self.az[
            "last_metadata_modification_date_dt"
        ].dt.strftime("%Y-%m-%d")

        self.az = self.az.drop(
            columns=[
                "last_content_modification_date_dt",
                "last_metadata_modification_date_dt",
            ]
        )

    def get_extension_mimetype(self):
        self.az["extension"] = self.az["name"].str.extract(r".*(\..*)$")
        self.az["mimetype"] = self.az["name"].apply(get_mimetype)
        self.az["guessed_extension"] = self.az["mimetype"].apply(
            get_extension_by_mimetype
        )

        self.az["file_type"] = self.az["mimetype"]
        self.az.loc[self.az["file_type"] == "image/jpeg", "file_type"] = "jpeg"
        self.az.loc[self.az["extension"] == ".jp2", "file_type"] = "jpeg2000"
        self.az.loc[self.az["file_type"] == "image/tiff", "file_type"] = "tiff"
        self.az.loc[self.az["file_type"] == "text/plain", "file_type"] = "ocr txt"
        self.az.loc[self.az["file_type"] == "application/pdf", "file_type"] = "pdf"
        self.az.loc[self.az["file_type"] == "application/xml", "file_type"] = "ocr xml"
        self.az.loc[self.az["extension"] == ".alto", "file_type"] = "ocr xml"
        self.az.loc[self.az["file_type"].str[:4] == "vide", "file_type"] = "video"
        self.az.loc[self.az["extension"] == ".MTS", "file_type"] = "video"
        self.az.loc[self.az["file_type"].str[:4] == "audi", "file_type"] = "audio"
        self.az.loc[
            ~self.az["file_type"].isin(
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
            "file_type",
        ] = "autre"

    def get_jhove_chunk(self, n=500):
        df2chunk = self.az[["bnr_file_id", "file_type"]][
            self.az["file_type"].isin(["jpeg", "jpeg2000", "tiff", "pdf"])
        ]
        df = df2chunk[["bnr_file_id"]]
        list_df = []
        j = 0
        for i in range(0, df.shape[0], n):
            j += 1
            j_str = str(j).zfill(8)
            new_df = df[i : i + n]
            new_df["jhove_chunk_id"] = f"{self.today}_{j_str}"
            list_df.append(new_df)
        df2chunk = pd.concat(list_df)
        self.az = self.az.merge(df2chunk, how="left", on="bnr_file_id")

    def save_az(self, filename, columns=None, format=None):
        if columns:
            self.az2export = self.az[columns]
        if format == "xlsx":
            self.az.to_excel(filename, index="False")
        else:
            self.az.to_csv(filename, index="False")


class Azrael2jhove_files:
    def __init__(self, **kwargs):
        self.today = datetime.now().strftime("%Y%m%d")
        if "jhove_path" in kwargs:
            self.jhove_path = kwargs.get("jhove_path")
        else:
            self.jhove_path = "../jhove/jhove"

    def create_az(self, min_jhove_chunk=100, max_jhove_chunk=100, **kwargs):
        if "path_prefix" in kwargs:
            self.path_prefix = kwargs.get("path_prefix")
        if "az" in kwargs:
            self.az = kwargs.get("az")
        elif "path_az" in kwargs:
            path_az = kwargs.get("path_az")
            self.az = pd.read_csv(path_az)
        self.az = self.az.sort_values(by=["path", "name"])
        if hasattr(self, "path_prefix"):
            self.az["path"] = self.az["path"].str.replace(self.path_prefix, "")
        self.az = self.az[["bnr_file_id", "path", "name", "jhove_chunk"]]
        self.az = self.az[~self.az["jhove_chunk"].isna()]
        self.az["jhove_chunk_id"] = self.az["jhove_chunk"].str[-8:].astype(int)
        self.az = self.az[self.az["jhove_chunk_id"] > min_jhove_chunk]
        self.az = self.az[self.az["jhove_chunk_id"] <= max_jhove_chunk]
        self.az["full_path"] = (
            self.path_prefix + self.az["path"] + "/" + self.az["name"]
        )

    def jhove_proc(self):
        grouped = self.az.groupby("jhove_chunk")
        for name, group in grouped:
            print(datetime.now())
            print(name)
            pathes = group["full_path"].to_list()
            jhove_execution(pathes, name, self.jhove_path)


class Azrael2exiftool_files:
    def __init__(self, **kwargs):
        self.today = datetime.now().strftime("%Y%m%d")

    def create_az(self, min_bnr_file_id=1, max_bnr_file_id=100, **kwargs):
        if "path_prefix" in kwargs:
            self.path_prefix = kwargs.get("path_prefix")
        if "date_extraction" in kwargs:
            self.date_extraction = kwargs.get("date_extraction")
        if "az" in kwargs:
            self.az = kwargs.get("az")
        elif "path_az" in kwargs:
            path_az = kwargs.get("path_az")
            self.az = pd.read_csv(path_az)
        self.az = self.az.sort_values(by=["path", "name"])
        if hasattr(self, "path_prefix"):
            self.az["path"] = self.az["path"].str.replace(self.path_prefix, "")
        self.az = self.az[["bnr_file_id", "path", "name"]]
        self.az = self.az[~self.az["bnr_file_id"].isna()]
        self.az["bnr_file_id_int"] = self.az["bnr_file_id"].str[-8:].astype(int)
        self.az = self.az[self.az["bnr_file_id_int"] >= min_bnr_file_id]
        self.az = self.az[self.az["bnr_file_id_int"] <= max_bnr_file_id]
        self.az["full_path"] = (
            self.path_prefix + self.az["path"] + "/" + self.az["name"]
        )

    def exiftool_proc(self):
        for file_data in self.az.to_dict(orient="records"):
            print(datetime.now())
            print(file_data["full_path"])
            with ExifToolHelper() as et:
                d = et.get_metadata(file_data["full_path"])
                file_out = join(
                    data,
                    f"exiftool_{self.date_extraction}_{file_data['bnr_file_id']}.json",
                )
                with open(file_out, mode="w", encoding="utf-8") as write_file:
                    json.dump(d, write_file)


class Jhove2csv:
    def __init__(self, **kwargs):
        if "jhove_file" in kwargs:
            self.jhove_file = kwargs.get("jhove_file")
            self.ns = {
                "jhove": "http://schema.openpreservation.org/ois/xml/ns/jhove",
                "mix": "http://www.loc.gov/mix/v20",
            }
            self.tree = etree.parse(self.jhove_file)
        if "path_prefix" in kwargs:
            self.path_prefix = kwargs.get("path_prefix")
        else:
            self.path_prefix = "/home/kibini/bnr/"

    def jhove_parser(self):
        self.results = []
        for repinfo in self.tree.xpath("//jhove:repInfo", namespaces=self.ns):
            metadata = {}
            metadata["jhove_uri"] = repinfo.get("uri")
            metadata["jhove_uri"] = metadata["jhove_uri"].replace(self.path_prefix, "")

            for lastModified in repinfo.xpath(
                "./jhove:lastModified", namespaces=self.ns
            ):
                metadata["jhove_lastModified"] = lastModified.text
            for size in repinfo.xpath("./jhove:size", namespaces=self.ns):
                metadata["jhove_size"] = size.text
            for format in repinfo.xpath("./jhove:format", namespaces=self.ns):
                metadata["jhove_format"] = format.text
            for version in repinfo.xpath("./jhove:version", namespaces=self.ns):
                metadata["jhove_version"] = version.text
            for mimeType in repinfo.xpath("./jhove:mimeType", namespaces=self.ns):
                metadata["jhove_mimeType"] = mimeType.text

            for mix in repinfo.xpath("descendant::*/mix:mix", namespaces=self.ns):
                for imageWidth in mix.xpath(
                    "descendant::mix:imageWidth", namespaces=self.ns
                ):
                    metadata["jhove_imageWidth"] = imageWidth.text
                for imageHeight in mix.xpath(
                    "descendant::mix:imageHeight", namespaces=self.ns
                ):
                    metadata["jhove_imageHeight"] = imageHeight.text

                for spatial_metrics in mix.xpath(
                    "descendant::mix:ImageAssessmentMetadata/mix:SpatialMetrics",
                    namespaces=self.ns,
                ):
                    for samplingFrequencyUnit in spatial_metrics.xpath(
                        "./mix:samplingFrequencyUnit", namespaces=self.ns
                    ):
                        metadata["jhove_samplingFrequencyUnit"] = (
                            samplingFrequencyUnit.text
                        )

                    for xsamplingfrequency in spatial_metrics.xpath(
                        "descendant::mix:xSamplingFrequency", namespaces=self.ns
                    ):
                        x_numerator_res = None
                        x_denominator_res = None
                        x_res = None
                        for x_numerator in xsamplingfrequency.xpath(
                            "./mix:numerator", namespaces=self.ns
                        ):
                            x_numerator_res = x_numerator.text
                        for x_denominator in xsamplingfrequency.xpath(
                            "./mix:denominator", namespaces=self.ns
                        ):
                            x_denominator_res = x_denominator.text
                        if x_numerator_res and x_denominator_res:
                            metadata["jhove_x_resolution"] = int(x_numerator_res) / int(
                                x_denominator_res
                            )
                        if x_numerator_res and x_denominator_res == None:
                            metadata["jhove_x_resolution"] = int(x_numerator_res)

                    for ysamplingfrequency in spatial_metrics.xpath(
                        "descendant::mix:ySamplingFrequency", namespaces=self.ns
                    ):
                        y_numerator_res = None
                        y_denominator_res = None
                        y_res = None
                        for y_numerator in ysamplingfrequency.xpath(
                            "./mix:numerator", namespaces=self.ns
                        ):
                            y_numerator_res = y_numerator.text
                        for y_denominator in ysamplingfrequency.xpath(
                            "./mix:denominator", namespaces=self.ns
                        ):
                            y_denominator_res = y_denominator.text
                        if y_numerator_res and y_denominator_res:
                            metadata["jhove_y_resolution"] = int(y_numerator_res) / int(
                                y_denominator_res
                            )
                        if y_numerator_res and y_denominator_res == None:
                            metadata["jhove_y_resolution"] = int(y_numerator_res)

            self.results.append(metadata)

    def save_results(self, filename):
        results_df = pd.DataFrame(
            self.results,
            columns=[
                "jhove_uri",
                "jhove_lastModified",
                "jhove_format",
                "jhove_version",
                "jhove_mimeType",
                "jhove_size",
                "jhove_imageWidth",
                "jhove_imageHeight",
                "jhove_samplingFrequencyUnit",
                "jhove_x_resolution",
                "jhove_y_resolution",
            ],
        )
        results_df.to_csv(filename, index=False)

    def results2df(self, chunk_id=None):
        self.results_df = pd.DataFrame(
            self.results,
            columns=[
                "jhove_uri",
                "jhove_lastModified",
                "jhove_format",
                "jhove_version",
                "jhove_mimeType",
                "jhove_size",
                "jhove_imageWidth",
                "jhove_imageHeight",
                "jhove_samplingFrequencyUnit",
                "jhove_x_resolution",
                "jhove_y_resolution",
            ],
        )
        if chunk_id:
            self.results_df["jhove_chunk_id"] = chunk_id


if __name__ == "__main__":
    jhove_file = "data/jhove_chunk_20240611_00000010.xml.gz"
    jhove2csv = Jhove2csv(jhove_file=jhove_file)
    jhove2csv.jhove_parser()
    jhove2csv.save_results("data/jhove_chunk_20240611_00000010.csv")
