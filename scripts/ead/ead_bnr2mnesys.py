from html import unescape
from os.path import exists, join

import pandas as pd
from lxml import etree


class EADbnr2mnesys:
    """
    Classe pour transformer des fichiers EAD (Encoded Archival Description) des IR BnR
    en vue de leur import dans Mnesys.
    """

    def __init__(self, names_csv_path, oai_csv_path, irs_excel_path):
        """
        Initialise le transformateur avec les chemins vers les fichiers de données.

        Args:
            names_csv_path (str): Chemin vers le CSV contenant les noms (persname/corpname).
            oai_csv_path (str): Chemin vers le CSV contenant les correspondances OAI (cote -> osiros_id).
            irs_excel_path (str): Chemin vers le fichier Excel listant les instruments de recherche.
        """
        self.names_type = self._load_names(names_csv_path)
        self.oai_dict = self._load_oai(oai_csv_path)
        self.irs = self._load_irs(irs_excel_path)

        # Dossiers de travail
        self.input_dir = join("data", "ead", "bnr")
        self.output_dir = join("results", "ead_cor", "bnr2mnesys")

    def _load_names(self, csv_path):
        """Charge les noms (persname/corpname) depuis un CSV."""
        names = pd.read_csv(csv_path)
        return {
            "p": names[names["type"] == "persname"]["contenu"].tolist(),
            "c": names[names["type"] == "corpname"]["contenu"].tolist(),
        }

    def _load_oai(self, csv_path):
        """Charge les correspondances OAI (cote -> osiros_id) depuis un CSV."""
        oai = pd.read_csv(csv_path)
        oai = oai[~oai["cote"].isna()]
        return dict(zip(oai["cote"], oai["osiros_id"]))

    def _load_irs(self, excel_path):
        """Charge la liste des instruments de recherche depuis un Excel."""
        irs = pd.read_excel(excel_path)
        return irs[irs["statut"] == "TRANSFERER"]

    def _find_ancestor(self, context, tag):
        """Trouve l'ancêtre d'un élément avec une balise donnée."""
        ancestors = context.xpath(f"ancestor::{tag}")
        return ancestors[0] if ancestors else None

    def _is_element_empty(self, element):
        """Vérifie si un élément XML est vide."""
        if element.text and element.text.strip() != "":
            return False
        if len(element.attrib) > 0:
            return False
        for child in element:
            if not self._is_element_empty(child):
                return False
        return True

    def _add_osiros_id(self, element):
        """Ajoute l'attribut `osiros_id` aux éléments `<c>` sur la base d'un fichier de
        concordance : unitid / osiros_id. Cet attribut permettra de construire un ark à maintenir,
        correspondant à l'ancienne bn-r.
        Attention, on est bien conscients que cet attribut n'existe pas dans l'ead."""
        for c in element.findall(".//c"):
            unitid = c.find("./did/unitid")
            if unitid is not None and unitid.text in self.oai_dict:
                c.set("osiros_id", self.oai_dict[unitid.text])

    def _add_dao_from_osiros_id(self, element):
        """
        Gère les balises <dao> et <daogrp> pour chaque élément <c> avec un attribut osiros_id.
        Trois cas principaux :
        1. Si <daogrp> existe : ajoute un <daoloc> dedans.
        2. Si <dao> existe (sans <daogrp>) : transforme en <daogrp><daoloc> et ajoute le nouveau <daoloc>.
        3. Sinon : crée un <dao> simple.
        Les éléments sont insérés avant le premier enfant <c> s'il existe.
        """
        for c in element.findall(".//c"):
            if "osiros_id" in c.attrib:
                osiros_id = c.attrib["osiros_id"]

                # Trouver la position d'insertion : avant le premier enfant <c> s'il existe
                first_child_c = None
                for child in c:
                    if child.tag == "c":
                        first_child_c = child
                        break

                # Cas 1 : <daogrp> existe déjà
                daogrp = c.find("daogrp")
                if daogrp is not None:
                    # Vérifier si un <daoloc> avec role="old_ark" existe déjà
                    old_ark_daoloc_exists = any(
                        daoloc.get("role") == "old_ark"
                        for daoloc in daogrp.findall("daoloc")
                    )
                    if not old_ark_daoloc_exists:
                        new_daoloc = etree.SubElement(daogrp, "daoloc")
                        new_daoloc.set("href", osiros_id)
                        new_daoloc.set("role", "old_ark")

                # Cas 2 : <dao> existe mais pas <daogrp>
                elif c.find("dao") is not None:
                    # Créer <daogrp> et <daoloc>
                    daogrp = etree.Element("daogrp")
                    daoloc = etree.SubElement(daogrp, "daoloc")

                    # Déplacer l'ancienne <dao> dans <daoloc>
                    old_dao = c.find("dao")
                    daoloc.append(old_dao)

                    # Ajouter la nouvelle <daoloc> pour osiros_id
                    new_daoloc = etree.SubElement(daogrp, "daoloc")
                    new_daoloc.set("href", osiros_id)
                    new_daoloc.set("role", "old_ark")

                    # Insérer <daogrp> avant le premier enfant <c> ou à la fin
                    if first_child_c is not None:
                        index = list(c).index(first_child_c)
                        c.insert(index, daogrp)
                    else:
                        c.append(daogrp)

                    # Supprimer l'ancienne <dao> (déjà déplacée)
                    c.remove(old_dao)

                # Cas 3 : Ni <dao> ni <daogrp> n'existe
                else:
                    new_dao = etree.Element("dao")
                    new_dao.set("href", osiros_id)
                    new_dao.set("role", "old_ark")

                    # Insérer <dao> avant le premier enfant <c> ou à la fin
                    if first_child_c is not None:
                        index = list(c).index(first_child_c)
                        c.insert(index, new_dao)
                    else:
                        c.append(new_dao)

    def _strip_whitespace(self, element):
        """Supprime les espaces en début/fin de texte dans les éléments `<controlaccess>`."""
        for controlaccess in element.xpath("//controlaccess//*"):
            if controlaccess.text is not None:
                controlaccess.text = controlaccess.text.strip()
            if controlaccess.tail is not None:
                controlaccess.tail = controlaccess.tail.strip()

    def _remove_empty_attributes(self, element):
        """Supprime les attributs vides dans tout l'arbre XML."""
        for elem in element.iter():
            empty_attrs = [attr for attr, value in elem.attrib.items() if value == ""]
            for attr in empty_attrs:
                del elem.attrib[attr]

    def _remove_empty_elements(self, element):
        """Supprime les éléments vides (récursivement)."""
        for elem in list(element.iter()):
            if self._is_element_empty(elem):
                parent = elem.getparent()
                if parent is not None:
                    parent.remove(elem)

    def _remove_html_entities(self, element):
        """Décode les entités HTML dans le texte et les queues des éléments."""
        for e in element.iter():
            if e.text:
                e.text = unescape(e.text)
            if e.tail:
                e.tail = unescape(e.tail)

    def _remove_name(self, element):
        """Remplace les balises `<name>` par `<persname>` ou `<corpname>` selon une liste
        préparée en amont."""
        for controlaccess in element.xpath("//controlaccess"):
            for name in controlaccess.xpath(".//name"):
                text = name.text.strip() if name.text else ""
                if text in self.names_type["c"]:
                    name.tag = "corpname"
                elif text in self.names_type["p"]:
                    name.tag = "persname"

    def _move_origination(self, element):
        """
        Déplace les éléments `<name>`/`<persname>` de `<origination>` vers `<controlaccess>`.
        Supprime `<origination>` s'il devient vide.
        """
        for origination in element.xpath("//origination"):
            name_elements = origination.xpath(".//name | .//persname")
            parent = origination.getparent()
            archdesc = (
                self._find_ancestor(origination, "archdesc") or parent.getparent()
            )

            controlaccess = parent.getparent().find("controlaccess")
            if controlaccess is None:
                controlaccess = etree.SubElement(parent.getparent(), "controlaccess")

            for e in name_elements:
                if e.tag == "name":
                    text = e.text.strip() if e.text else ""
                    if text in self.names_type["p"]:
                        new_tag = "persname"
                    elif text in self.names_type["c"]:
                        new_tag = "corpname"
                    else:
                        new_tag = e.tag
                    new_elem = etree.Element(new_tag)
                    new_elem.text = e.text
                    new_elem.attrib.update(e.attrib)
                    controlaccess.append(new_elem)
                else:
                    controlaccess.append(e)
                e.getparent().remove(e)

            if self._is_element_empty(origination):
                origination.getparent().remove(origination)

    def _update_eadid(self, element, new_eadid):
        """Met à jour la balise `<eadid>`."""
        eadid = element.find(".//eadid")
        if eadid is not None:
            eadid.text = new_eadid

    def _update_archdesc_unitid(self, element, new_unitid):
        """Met à jour la balise `<unitid>` dans `<archdesc/did>`."""
        unitid = element.find("./archdesc/did/unitid")
        if unitid is not None:
            unitid.text = new_unitid

    def _update_repository(self, element, new_repository):
        """Met à jour la balise `<repository>` dans `<archdesc/did>`."""
        repository = element.find("./archdesc/did/repository")
        if repository is not None:
            repository.text = new_repository

    def _remove_repositories(self, element):
        """Supprime les balises `<repository>` en dehors de `<archdesc/did>`."""
        for repo in element.xpath("//repository"):
            parent = repo.getparent()
            if (
                parent is None
                or parent.tag != "did"
                or (
                    parent.getparent() is not None
                    and parent.getparent().tag != "archdesc"
                )
            ):
                repo.getparent().remove(repo)

    def _update_controlaccess_source(self):
        pass

    def transform_ead(self, ir):
        """
        Applique toutes les transformations à un fichier EAD.

        Args:
            ir (dict): Dictionnaire contenant les métadonnées de l'instrument de recherche.
        """
        input_path = join(self.input_dir, ir["nom_fichier"])
        output_path = join(self.output_dir, ir["nouveau_nom_fichier"])

        if not exists(input_path):
            print(f"Fichier introuvable : {input_path}")
            return False

        try:
            tree = etree.parse(input_path)
            root = tree.getroot()
        except etree.XMLSyntaxError as e:
            print(f"XML invalide dans {input_path} : {e}")
            return False

        # Appliquer les transformations
        self._update_eadid(root, ir["nouveau_ead_id"])
        self._update_archdesc_unitid(root, ir["nouveau_archdesc_unitid"])
        self._update_repository(root, ir["nouveau_repository"])
        self._remove_html_entities(root)
        self._strip_whitespace(root)
        self._move_origination(root)
        self._add_osiros_id(root)
        self._add_dao_from_osiros_id(root)
        self._remove_name(root)
        self._remove_repositories(root)
        # self._update_controlaccess_source(root)
        self._remove_empty_attributes(root)
        self._remove_empty_elements(root)

        # Sauvegarder le fichier transformé
        tree.write(
            output_path,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
            with_comments=True,
        )
        return True

    def run(self):
        """Exécute la transformation pour tous les instruments de recherche."""
        for ir in self.irs.to_dict(orient="records"):
            self.transform_ead(ir)


# --- Utilisation ---
if __name__ == "__main__":
    transformer = EADbnr2mnesys(
        names_csv_path=join(
            "results", "ead", "indexation", "controlaccess_extraction_name2.csv"
        ),
        oai_csv_path=join("data", "oai", "oai_records_20260430.csv.gz"),
        irs_excel_path=join(
            "results",
            "ir",
            "liste_instruments_recherche_20260521_transfert_mnesys.xlsx",
        ),
    )
    transformer.run()
