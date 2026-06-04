import pandas as pd

from lxml import etree

from os.path import join
from os import listdir

indexation = []
for logiciel in ["bnr", "mnesys"]:
    ead_folder = join("data", "ead_source", logiciel)
    ead_files = [f for f in listdir(ead_folder)]
    for ead_file in ead_files:
        tree = etree.parse(join(ead_folder, ead_file))
        eadheader = tree.xpath("/ead/eadheader")[0]
        eadid = eadheader.xpath("//eadid")[0].text
        if eadheader.xpath("//titleproper"):
            ir_titleproper = eadheader.xpath("//titleproper")[0].text
            for s in tree.xpath("//persname"):
                parent = s.getparent()
                if "role" in s.attrib:
                    role = s.get('role')
                    persname = s.text
                else:
                    role = "N#A"
                    nom = s.text

                indexation.append(
                    {
                        'logiciel': logiciel,
                        'ead_file': ead_file,
                        'ir_titleproper': ir_titleproper,
                        'role': role,
                        'nom': nom,
                        'parent': parent.tag
                    })

indexation_df = pd.DataFrame(indexation)
indexation_df.to_csv(join("analyses", "indexation", "indexation_nom_pers_20251212.csv"), index=False)
indexation_df['nb'] = 1
indexation_df_gr = indexation_df.pivot_table(
    index='nom',
    columns = 'role',
    values = 'nb',
    aggfunc='sum',
    margins=True,
    margins_name = 'Total')
indexation_df_gr.reset_index().to_excel(join("analyses", "indexation", "indexation_nom_pers_20251212_grp.xlsx"), index=False)
