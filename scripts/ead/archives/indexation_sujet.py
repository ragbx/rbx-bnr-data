from os import listdir
from os.path import join

import pandas as pd
from lxml import etree

indexation = []
for logiciel in ["bnr"]:
    ead_folder = join("results", "ead_cor", "bnr2mnesys")
    ead_files = [f for f in listdir(ead_folder)]
    for ead_file in ead_files:
        tree = etree.parse(join(ead_folder, ead_file))
        eadheader = tree.xpath("/ead/eadheader")[0]
        eadid = eadheader.xpath("//eadid")[0].text
        if eadheader.xpath("//titleproper"):
            ir_titleproper = eadheader.xpath("//titleproper")[0].text
            for s in tree.xpath("//subject"):
                parent = s.getparent()
                if "source" in s.attrib:
                    source = s.get("source")
                    subject = s.text
                else:
                    source = "INCONNUE"
                    subject = s.text

                indexation.append(
                    {
                        "logiciel": logiciel,
                        "ead_file": ead_file,
                        "ir_titleproper": ir_titleproper,
                        "source": source,
                        "sujet": subject,
                        "parent": parent.tag,
                    }
                )

indexation_df = pd.DataFrame(indexation)
indexation_df.to_csv(
    join("results", "ead", "indexation", "indexation_sujet.csv"), index=False
)
indexation_df["nb"] = 1
indexation_df_gr = indexation_df.pivot_table(
    index="sujet",
    columns="source",
    values="nb",
    aggfunc="sum",
    margins=True,
    margins_name="Total",
)
indexation_df_gr.reset_index().to_excel(
    join("results", "ead", "indexation", "indexation_sujet_grp.xlsx"), index=False
)
