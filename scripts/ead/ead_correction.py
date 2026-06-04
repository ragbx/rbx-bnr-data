from html import unescape
from os import listdir
from os.path import join

import pandas as pd
from lxml import etree

"""
Script qui permet de préparer les IR bn-r à un import dans Mnesys.
Les modifications suivantes sont réalisées :
    - renommage du fichier
    - MaJ eadid
    - MaJ archdesc/did/unitid
    - MaJ repository
    - suppression des attributs vides
    - suppression des éléments vides
    - suppression des entités html
    - suppression des caractères vides en fin de texte dans controlaccess
"""


def strip_whitespace(element):
    for controlaccess in element.xpath("//controlaccess//*"):
        if controlaccess.text is not None:
            controlaccess.text = controlaccess.text.strip()
        if controlaccess.tail is not None:
            controlaccess.tail = controlaccess.tail.strip()


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


def remove_html_entities(element):
    for e in element.iter():
        if e.text:
            e.text = unescape(e.text)
        if e.tail:
            e.tail = unescape(e.tail)


def clean_html_entities(text):
    if text is None:
        return ""
    # Remplace les entités HTML courantes
    entity_map = {
        "&amp;": "&",
        "&lt;": "<",
        "&gt;": ">",
        "&eacute;": "é",
        "&egrave;": "è",
        "&ecirc;": "ê",
        "&agrave;": "à",
        "&acirc;": "â",
        "&ocirc;": "ô",
        "&ucirc;": "û",
        "&ccedil;": "ç",
        "&#13;": "\n",
        "&nbsp;": " ",
        "&brvbar;": "|",
    }

    for entity, char in entity_map.items():
        text = text.replace(entity, char)
    return text


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
                new_persname = etree.Element("name")  # on garde provisoirement le name
                new_persname.text = e.text
                new_persname.attrib.update(e.attrib)
                controlaccess.append(new_persname)
            else:
                controlaccess.append(e)

            # Supprimer l'élément original
            e.getparent().remove(e)


def update_eadid(element, new_eadid):
    eadid = element.find(".//eadid")
    if eadid is not None:
        eadid.text = new_eadid


def update_archdesc_unitid(element, new_archdesc_unitid):
    unitid = element.find("./archdesc/did/unitid")
    if unitid is not None:
        unitid.text = new_archdesc_unitid


def update_repository(element, new_repository):
    repository = element.find("./archdesc/did/repository")
    if repository is not None:
        repository.text = new_repository


def transform_ead(ir):
    ir_filename = join("data", "ead", "bnr", ir["nom_fichier"])
    new_ir_filename = join(
        "results", "ead_cor", "bnr2mnesys", ir["nouveau_nom_fichier"]
    )

    tree = etree.parse(ir_filename)
    root = tree.getroot()
    update_eadid(root, ir["nouveau_ead_id"])
    update_archdesc_unitid(root, ir["nouveau_archdesc_unitid"])
    update_repository(root, ir["nouveau_repository"])
    remove_html_entities(root)
    strip_whitespace(root)
    move_origination(root)
    remove_empty_attributes(root)
    remove_empty_elements(root)

    # Sauvegarder le fichier transformé
    tree.write(
        new_ir_filename,
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
    transform_ead(ir)
    print(ir)
