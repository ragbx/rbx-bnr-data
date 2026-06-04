import html
from os import listdir
from os.path import join

import pandas as pd
from lxml import etree

"""
Script qui permet de préparer les IR bn-r à un import dans Mnesys.
Les modifications suivantes sont réalisées :
    - renommage du fichier
    - MaJ eadid
    -
"""


def remove_empty_attributes(element):
    """
    Supprime récursivement tous les attributs vides (valeur = "") d'un élément et de ses enfants.
    Modifie l'arbre XML en place.
    """
    # Parcourir tous les éléments de l'arbre
    for elem in element.iter():
        # Créer une liste des attributs à supprimer (pour éviter de modifier le dict pendant l'itération)
        empty_attrs = [attr for attr, value in elem.attrib.items() if value == ""]
        for attr in empty_attrs:
            del elem.attrib[attr]


def is_element_empty(element):
    """
    Vérifie si un élément est vide :
    - Pas de texte (ou texte vide/blanc).
    - Tous ses enfants sont vides (récursivement).
    - Pas d'attributs.
    """
    # Vérifier le texte de l'élément
    if element.text and element.text.strip() != "":
        return False

    # Vérifier les attributs
    if len(element.attrib) > 0:
        return False

    # Vérifier récursivement les enfants
    for child in element:
        if not is_element_empty(child):
            return False

    return True


def remove_empty_elements(element):
    """
    Supprime récursivement tous les éléments vides (y compris ceux qui ne contiennent que des éléments vides).
    Modifie l'arbre en place.
    """
    # Parcourir tous les éléments en ordre inverse
    for elem in list(element.iter()):
        if is_element_empty(elem):
            parent = elem.getparent()
            if parent is not None:
                parent.remove(elem)


def move_origination(element):
    # Trouver tous les éléments <origination>
    for origination in element.xpath("//origination"):
        name_elements = origination.xpath(".//name | .//persname")

        for e in name_elements:
            # Trouver le parent de <origination> (ex: <did>)
            parent = origination.getparent()

            # Trouver <controlaccess> au même niveau que <did>
            controlaccess = parent.getparent().find("controlaccess")

            # Si <controlaccess> n'existe pas, le créer sous <archdesc> ou le parent de <did>
            if controlaccess is None:
                controlaccess = etree.SubElement(parent.getparent(), "controlaccess")

            # Renommer <name> en <persname> si nécessaire
            if e.tag == "name":
                new_persname = etree.Element("persname")
                new_persname.text = e.text
                new_persname.attrib.update(e.attrib)
                controlaccess.append(new_persname)
            else:
                controlaccess.append(e)

            # Supprimer l'élément original
            e.getparent().remove(e)


def transform_ead(input_file, output_file):
    tree = etree.parse(input_file)
    root = tree.getroot()
    move_origination(root)
    remove_empty_attributes(root)
    remove_empty_elements(root)

    # Sauvegarder le fichier transformé
    tree.write(
        output_file,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=True,
        with_comments=True,
    )


irs = pd.read_excel(
    join("results", "ir", "liste_instruments_recherche_20260521_transfert_mnesys.xlsx")
)
irs = irs[irs["statut"] == "TRANSFERER"]

for ir in irs.to_dict(orient="records"):
    ir_filename = join("data", "ead", "bnr", ir["nom_fichier"])
    new_ir_filename = join(
        "results", "ead_cor", "bnr2mnesys", ir["nouveau_nom_fichier"]
    )
    transform_ead(ir_filename, new_ir_filename)
    print(new_ir_filename)
