import html
from datetime import datetime
from os import listdir
from os.path import join

import pandas as pd
from lxml import etree

today = datetime.now().strftime("%Y%m%d")

results = []
for source in ["bnr"]:
    ead_folder = join("data", "ead", source)
    ead_files = [
        f
        for f in listdir(ead_folder)
        if ("_prettified" not in f) and ("_corr" not in f)
    ]

    for ead_file in ead_files:
        metadata = {"source": source, "file": ead_file}
        tree = etree.parse(join(ead_folder, ead_file))
        eadheader = tree.xpath("/ead/eadheader")[0]
        metadata["eadid"] = eadheader.xpath("//eadid")[0].text
        if eadheader.xpath("//titleproper"):
            metadata["titleproper"] = eadheader.xpath("//titleproper")[0].text
        archdesc = tree.xpath("/ead/archdesc")[0]
        if archdesc.xpath("//did/unitid"):
            metadata["archdesc_unitid"] = archdesc.xpath("//did/unitid")[0].text
        if archdesc.xpath("//did/repository"):
            metadata["repository"] = archdesc.xpath("//did/repository")[0].text
        results.append(metadata)

df = pd.DataFrame(results)
df.sort_values(by="file").to_excel(
    join("results", "ir", f"liste_instruments_recherche_{today}.xlsx"),
    index=False,
)
