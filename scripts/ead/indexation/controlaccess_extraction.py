from os import listdir
from os.path import join

import pandas as pd
from lxml import etree

ead_folder = join("results", "ead_cor", "bnr2mnesys")
ead_files = [f for f in listdir(ead_folder)]

results = []
for ead_file in ead_files:
    tree = etree.parse(join(ead_folder, ead_file))
    root = tree.getroot()

    eadid = (
        root.xpath("//eadheader/eadid/text()")[0]
        if root.xpath("//eadheader/eadid/text()")
        else None
    )
    titleproper = (
        root.xpath("//eadheader/filedesc/titlestmt/titleproper/text()")[0]
        if root.xpath("//eadheader/filedesc/titlestmt/titleproper/text()")
        else None
    )

    # Initialiser la liste des résultats

    # Trouver tous les éléments <controlaccess>
    controlaccess_elements = root.xpath("//controlaccess")

    for controlaccess in controlaccess_elements:
        for child in controlaccess:
            tag_name = child.tag
            text_content = child.text.strip() if child.text else None
            source = child.get("source")
            role = child.get("role")
            normal = child.get("normal")

            results.append(
                {
                    "eadid": eadid,
                    "titleproper": titleproper,
                    "balise": tag_name,
                    "concept": text_content,
                    "source": source,
                    "role": role,
                    "normal": normal,
                }
            )

df = pd.DataFrame(results)
df.to_csv(
    join("results", "ead", "indexation", "controlaccess_extraction.csv"), index=False
)
