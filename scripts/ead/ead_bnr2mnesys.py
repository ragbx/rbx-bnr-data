"""
Transformation de fichiers EAD BnR pour import dans Mnesys.

Ce script applique une série de transformations sur des fichiers XML EAD (Encoded Archival
Description) produits par la Bibliothèque numérique de Roubaix, en vue de leur import dans
le logiciel d'archivistique Mnesys.

Données d'entrée
----------------
- Un CSV de noms typés (persname / corpname) pour reclasser les balises <name>.
- Un CSV de correspondances cote → osiros_id issu de l'OAI, pour construire les anciens ARK BnR.
- Un fichier Excel listant les instruments de recherche à traiter (filtrés sur statut = "TRANSFERER").
- Les fichiers EAD source dans data/ead/bnr/.

Résultats
---------
Les fichiers EAD transformés sont écrits dans results/ead_cor/bnr2mnesys/.

Pipeline de transformations
---------------------------
Les transformations sont appliquées dans l'ordre suivant sur chaque fichier EAD :

1. Mise à jour des métadonnées de l'instrument de recherche
   - <eadid> : remplacé par la nouvelle valeur issue du fichier Excel.
   - <archdesc/did/unitid> : remplacé par le nouveau identifiant de l'IR.
   - <archdesc/did/repository> : remplacé par le nouveau nom du service versant.

2. Nettoyage du contenu textuel
   - Décodage des entités HTML (&amp;, &lt;, etc.) dans tout l'arbre XML.
   - Suppression des espaces en début/fin de texte dans les enfants de <controlaccess>.

3. Réorganisation des accès (indexation)
   - <origination> : les éléments <name> et <persname> qu'il contient sont déplacés dans
     <controlaccess> (créé si absent). <origination> est supprimé s'il devient vide.
     Lors du déplacement, les <name> sont reclassés en <persname> ou <corpname> selon la
     liste CSV si une correspondance est trouvée.

4. Ajout des anciens ARK BnR
   - Pour chaque élément <c> dont le <unitid> figure dans la table de correspondance OAI,
     une balise pointant vers l'ancien ARK BnR (https://www.bn-r.fr/ark:/20179/<osiros_id>)
     est insérée selon trois cas :
       * <daogrp> déjà présent → ajout d'un <daoloc role="publication:previous"> dans le groupe.
       * <dao> présent (sans <daogrp>) → transformation en <daogrp> contenant un <daoloc>
         reprenant les attributs de l'ancienne <dao> et un <daoloc role="publication:previous">.
       * Ni <dao> ni <daogrp> → création d'un <dao role="publication:previous"> pointant vers l'ARK.
     Dans les cas 2 et 3, la balise est insérée avant le premier enfant <c> s'il existe.

5. Mise à jour des rôles des <dao>
   - Pour tous les éléments <dao> et <daoloc> dont l'attribut role commence par "image",
     le préfixe "access:" est ajouté devant la valeur existante.

6. Reclassement des balises <name>
   - Dans <controlaccess>, les balises <name> sont remplacées par <persname> ou <corpname>
     selon la liste CSV. Les <name> sans correspondance sont laissés tels quels.

7. Suppression des <repository> hors contexte
   - Toutes les balises <repository> situées en dehors de <archdesc/did> sont supprimées.

8. Nettoyage final
   - Suppression des attributs dont la valeur est une chaîne vide.
   - Suppression récursive des éléments XML vides (sans texte, sans attribut, sans enfant
     non vide).

Utilisation
-----------
    python ead_bnr2mnesys.py
"""
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

    def _add_dao_ark(self, element):
        """
        Ajoute ou modifie les balises <dao> ou <daogrp> pour chaque élément <c> dont le
        <unitid> figure dans la table de correspondance OAI, afin de conserver un lien vers
        l'ancien ARK BnR. Trois cas selon la structure existante :

        - <daogrp> présent : ajout d'un <daoloc role="publication:previous"> dans le groupe existant
          (sans doublon).
        - <dao> présent (sans <daogrp>) : transformation en <daogrp> avec un <daoloc>
          reprenant les attributs de l'ancienne <dao> et un <daoloc role="publication:previous">.
        - Ni <dao> ni <daogrp> : création d'un <dao role="publication:previous">.

        Dans les cas 2 et 3, la balise est insérée avant le premier enfant <c> s'il existe.
        """
        for c in element.findall(".//c"):
            unitid = c.find("./did/unitid")
            if unitid is None or unitid.text not in self.oai_dict:
                continue

            url = f"https://www.bn-r.fr/ark:/20179/{self.oai_dict[unitid.text]}"
            first_child_c = next((child for child in c if child.tag == "c"), None)

            # Cas 1 : <daogrp> existe déjà
            daogrp = c.find("daogrp")
            if daogrp is not None:
                if not any(daoloc.get("role") == "publication:previous" for daoloc in daogrp.findall("daoloc")):
                    new_daoloc = etree.SubElement(daogrp, "daoloc")
                    new_daoloc.set("href", url)
                    new_daoloc.set("role", "publication:previous")

            # Cas 2 : <dao> existe mais pas <daogrp>
            elif (old_dao := c.find("dao")) is not None:
                # Créer <daogrp>
                daogrp = etree.Element("daogrp")

                # Créer un <daoloc> avec les attributs de l'ancienne <dao>
                daoloc_from_dao = etree.SubElement(daogrp, "daoloc")
                for attr, value in old_dao.attrib.items():
                    daoloc_from_dao.set(attr, value)

                # Ajouter la nouvelle <daoloc> pour osiros_id
                new_daoloc = etree.SubElement(daogrp, "daoloc")
                new_daoloc.set("href", url)
                new_daoloc.set("role", "publication:previous")

                # Insérer <daogrp> avant le premier enfant <c> ou à la fin
                if first_child_c is not None:
                    index = list(c).index(first_child_c)
                    c.insert(index, daogrp)
                else:
                    c.append(daogrp)

                # Supprimer l'ancienne <dao>
                c.remove(old_dao)

            # Cas 3 : Ni <dao> ni <daogrp> n'existe
            else:
                new_dao = etree.Element("dao")
                new_dao.set("href", url)
                new_dao.set("role", "publication:previous")

                if first_child_c is not None:
                    index = list(c).index(first_child_c)
                    c.insert(index, new_dao)
                else:
                    c.append(new_dao)


    def _update_dao_roles(self, element):
        """Préfixe 'access:' au role des <dao> et <daoloc> dont le role commence par 'image'."""
        for dao in element.xpath(".//*[self::dao or self::daoloc]"):
            role = dao.get("role", "")
            if role.startswith("image"):
                dao.set("role", f"access:{role}")

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
        self._add_dao_ark(root)
        self._update_dao_roles(root)
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
