from lxml import etree


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


def transform_ead(input_file, output_file):
    tree = etree.parse(input_file)
    root = tree.getroot()
    remove_empty_attributes(root)
    remove_empty_elements(root)

    # Trouver tous les éléments <origination>
    for origination in root.xpath("//origination"):
        name_elements = origination.xpath(".//name | .//persname")

        for element in name_elements:
            # Trouver le parent de <origination> (ex: <did>)
            parent = origination.getparent()

            # Trouver <controlaccess> au même niveau que <did>
            controlaccess = parent.getparent().find("controlaccess")

            # Si <controlaccess> n'existe pas, le créer sous <archdesc> ou le parent de <did>
            if controlaccess is None:
                controlaccess = etree.SubElement(parent.getparent(), "controlaccess")

            # Renommer <name> en <persname> si nécessaire
            if element.tag == "name":
                new_persname = etree.Element("persname")
                new_persname.text = element.text
                new_persname.attrib.update(element.attrib)
                controlaccess.append(new_persname)
            else:
                controlaccess.append(element)

            # Supprimer l'élément original
            element.getparent().remove(element)

    # Sauvegarder le fichier transformé
    tree.write(
        output_file,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=True,
        with_comments=True,
    )


# Exemple d'utilisation
transform_ead(
    "data/ead/bnr/FR595129901_MED_04.xml", f"results/ead_cor/test_origination.xml"
)
