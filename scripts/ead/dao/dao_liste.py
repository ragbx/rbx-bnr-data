import re
from datetime import datetime
from os import listdir
from os.path import join
from pathlib import Path

import pandas as pd
from lxml import etree

"""
script qui permet d'extraire les dao des IR
on en profite pour réaliser des stats sur les IR : nb de composants de dernier  niveau avec ou sans dao
on créée ensuite un fichier permettant de rechercher les fichiers correspondants aux dao
"""


def get_file_racine(filename):
    if isinstance(filename, str):
        filebase = Path(filename).stem
        pattern = "^RBX_([a-zA-Z0-9]+_[a-zA-Z0-9]+)_[a-zA-Z0-9]+"
        a = re.search(pattern, filebase)
        if a:
            racine = a.group(1)
        else:
            pattern = "^([a-zA-Z0-9]+_[a-zA-Z0-9]+)_[a-zA-Z0-9]+"
            a = re.search(pattern, filebase)
            if a:
                racine = a.group(1)
            else:
                racine = "INCONNU1"
    else:
        racine = "INCONNU0"

    # correction
    if "MUS_VAI" in racine:
        racine = "MUS_VAI"
    elif "LAI_" in racine:
        racine = "LAI"
    elif "AMR_OBJ" in racine:
        racine = "AMR_OBj"
    elif "AMR_PUV" in racine:
        racine = "AMR_PUV"
    elif "MeD_PAR" in racine:
        racine = "MED_PAR"

    return racine


def get_dao_base(dao):
    dao_base = Path(dao).stem
    return dao_base


def process_dsc_components(c, documents, composants):
    if c.xpath("*[(self::c)]"):
        for el in c.xpath("*[(self::c)]"):
            process_dsc_components(el, documents, composants)
    else:
        document = {
            "unitid": None,
            "dao": None,
            "daoloc_first": None,
            "daoloc_last": None,
        }

        composant = {"unitid": None, "dao": "sans dao"}

        if c.xpath("did/unitid"):
            unitid = c.xpath("did/unitid")[0].text
            document["unitid"] = unitid
            composant["unitid"] = unitid
        if c.xpath("dao"):
            document["dao"] = c.xpath("dao")[0].get("href")
            document["dao_racine"] = get_file_racine(document["dao"])
            composant["dao"] = "avec dao"
            composant["dao_racine"] = document["dao_racine"]
            documents.append(document)
        elif c.xpath("daogrp"):
            if c.xpath("daogrp/daoloc[@role='image:first']"):
                document["daoloc_first"] = c.xpath(
                    "daogrp/daoloc[@role='image:first']"
                )[0].get("href")
                document["dao_racine"] = get_file_racine(document["daoloc_first"])
                composant["dao_racine"] = document["dao_racine"]
                if c.xpath("daogrp/daoloc[@role='image:last']"):
                    document["daoloc_last"] = c.xpath(
                        "daogrp/daoloc[@role='image:last']"
                    )[0].get("href")
                documents.append(document)
            elif c.xpath("daogrp/daoloc"):
                for d in c.xpath("daogrp/daoloc"):
                    document["dao"] = d.get("href")
                    document["dao_racine"] = get_file_racine(document["dao"])
                    composant["dao"] = "avec dao"
                    composant["dao_racine"] = document["dao_racine"]
                    composant["dao"] = "avec dao"
                    documents.append(document)
        composants.append(composant)
    # return documents


date = datetime.now().strftime("%Y%m%d")

documents_df_list = []
composants_df_list = []
for source in ["bnr", "mnesys"]:
    ead_folder = join("results", "ead_cor", source)
    ead_files = [f for f in listdir(ead_folder)]
    for ead_file in ead_files:
        print(ead_file)
        tree = etree.parse(join(ead_folder, ead_file))
        eadheader = tree.xpath("/ead/eadheader")[0]
        eadid = eadheader.xpath("//eadid")[0].text
        if eadheader.xpath("//titleproper"):
            ir_titleproper = eadheader.xpath("//titleproper")[0].text
        ir_subtitle = "N/A"
        if eadheader.xpath("//subtitle"):
            ir_subtitle = eadheader.xpath("//subtitle")[0].text
            if ir_subtitle is None:
                ir_subtitle = "N/A"
        if tree.xpath("/ead/archdesc/did/unitid"):
            archdesc_unitid = tree.xpath("/ead/archdesc/did/unitid")[0].text
        # print(archdesc_unitid)
        dsc = tree.xpath("/ead/archdesc/dsc")[0]
        documents = []
        composants = []
        for c in dsc.xpath("c"):
            process_dsc_components(c, documents, composants)
        documents_df = pd.DataFrame(documents)
        documents_df["finding_aid"] = f"{source}_{ead_file}"
        documents_df_list.append(documents_df)
        print(len(documents_df))

        composants_df = pd.DataFrame(composants)
        composants_df["inventaire_fichier"] = f"{ead_file}"
        composants_df["inventaire_identifiant"] = eadid
        composants_df["inventaire_titre"] = ir_titleproper
        composants_df["inventaire_soustitre"] = ir_subtitle
        composants_df["archdesc_unitid"] = archdesc_unitid
        composants_df["inventaire_source"] = source
        composants_df_list.append(composants_df)
        print(len(composants_df))

documents_df_all = pd.concat(documents_df_list)
documents_df_all.to_csv(join("results", "dao", f"liste_dao_{date}.csv.gz"), index=False)

composants_df_all = pd.concat(composants_df_list)
p = composants_df_all.pivot_table(
    index=[
        "inventaire_source",
        "inventaire_fichier",
        "inventaire_identifiant",
        "inventaire_titre",
        "inventaire_soustitre",
        "archdesc_unitid",
    ],
    columns="dao",
    values="unitid",
    aggfunc="count",
    fill_value=0,
    margins=True,
    margins_name="Total",
).reset_index()
p2 = composants_df_all.pivot_table(
    index=[
        "inventaire_source",
        "inventaire_fichier",
        "inventaire_identifiant",
        "inventaire_titre",
        "inventaire_soustitre",
        "archdesc_unitid",
        "dao_racine",
    ],
    # columns='dao_racine',
    values="dao",
    aggfunc="count",
    fill_value=0,
    margins=True,
    margins_name="Total",
).reset_index()
p2 = p2[
    [
        "dao_racine",
        "dao",
        "inventaire_source",
        "inventaire_fichier",
        "inventaire_identifiant",
        "inventaire_titre",
        "inventaire_soustitre",
        "archdesc_unitid",
    ]
]
p2.columns = [
    "dao_racine",
    "nb_dao",
    "inventaire_source",
    "inventaire_fichier",
    "inventaire_identifiant",
    "inventaire_titre",
    "inventaire_soustitre",
    "archdesc_unitid",
]

with pd.ExcelWriter(join("results", "dao", f"liste_ir_statDao_{date}.xlsx")) as writer:
    p.to_excel(writer, sheet_name="par IR", index=False)
    p2.to_excel(writer, sheet_name="par racine", index=False)

# dernière étape :
dao = documents_df_all
# on sépare le fichier en dao_nuique et multiple
dao_unique = dao[~dao["dao"].isna()]
dao_unique["dao_base"] = dao_unique["dao"].apply(get_dao_base)

dao_multiple = dao[~dao["daoloc_first"].isna()]
dao_multiple = dao_multiple[~dao_multiple["daoloc_last"].isna()]
dao_multiple["daoloc_first_base"] = dao_multiple["daoloc_first"].apply(get_dao_base)
dao_multiple["daoloc_last_base"] = dao_multiple["daoloc_last"].apply(get_dao_base)
