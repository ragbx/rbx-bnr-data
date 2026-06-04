import html
from os import listdir
from os.path import join

from lxml import etree

for source in ["bnr", "mnesys"]:
    ead_folder = join("data", "ead", source)
    ead_files = [
        f
        for f in listdir(ead_folder)
        if ("_prettified" not in f) and ("_corr" not in f)
    ]
    for ead_file in ead_files:
        with open(join(ead_folder, ead_file), "r", encoding="utf-8") as f:
            content = f.read()
        if source == "bnr":
            elements_del = [
                "<address/>",
                "<author/>",
                "<editionstmt><edition/></editionstmt>",
                "<publisher/>",
                "<notestmt><note><p/></note></notestmt>",
                "<note><p/></note>",
                "<repository/>",
                "<physloc/>",
                '<otherfindaid><extref href=""/></otherfindaid>',
                '<otherfindaid><extref href=""></extref></otherfindaid>',
                '<resource><extref href=""></extref></resource>',
                "<scopecontent><p/></scopecontent>",
                '<resource><extref href=""/></resource>',
                "<phystech><p/></phystech>",
                "<physdesc/>",
                "<acqinfo><p/></acqinfo>",
                '<unitdate normal=""/>',
            ]
            for el in elements_del:
                content = content.replace(el, "")

            # """
            # La syntaxe liée à daogrp est à modifier au niveau de daoloc :
            #     saisir role= "first_image" et role="last_image".
            # NON
            # """

            # elements_mod = [
            #     ['role="image:first"', 'role="first_image"'],
            #     ['role="image:last"', 'role="last_image"'],
            # ]

            # for el in elements_mod:
            #     content = content.replace(el[0], el[1])

            # suppression des entités XML (2 passages sont nécessaires)
            content = html.unescape(content)
            content = html.unescape(content)

        # on remet des entités là où c'est nécessaire

        elements_mod = [
            ["&", "&amp;"],
            ['normal="K.S.M.P. "Wiosna""', 'normal="K.S.M.P. Wiosna"'],
        ]

        for el in elements_mod:
            content = content.replace(el[0], el[1])

        new_ead_folder = join("results", "ead_cor", source)
        new_ead_file = ead_file.replace(".xml", "_corr.xml")
        with open(join(new_ead_folder, new_ead_file), "w", encoding="utf-8") as f:
            f.write(content)

        tree = etree.parse(join(new_ead_folder, new_ead_file))
        # prettified_ead_file = ead_file.replace("_corr.xml", "_corr_prettified.xml")
        tree.write(
            join(new_ead_folder, new_ead_file),
            pretty_print=True,
            encoding="UTF-8",
            xml_declaration=True,
        )
