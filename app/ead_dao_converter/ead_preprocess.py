"""
Classe EAD_preprocess.
"""

import os
import sys
from pathlib import Path

from lxml import etree

# Réutilise la mécanique d'insertion des liens ARK partagée avec
# scripts/ead/ead_bnr2mnesys.py (dedup, cf. scripts/ead/dao_ark.py).
# parents[2] : app/ead_dao_converter/ead_preprocess.py -> app/ead_dao_converter -> app -> racine du dépôt.
_SCRIPTS_EAD = Path(__file__).resolve().parents[2] / "scripts" / "ead"
if str(_SCRIPTS_EAD) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_EAD))

from dao_ark import add_ark_links  # noqa: E402

# Rôles EAD reconnus dans les <p> d'un <odd> (cf. apply_odd_to_daoloc), tels que
# listés dans documentation/files/donnees/dao_daogrp.md (« Grammaire des role »).
ODD_ROLES = (
    "publication:current",
    "publication:previous",
    "access:image",
    "preservation:image",
    "access:image:first",
    "access:image:last",
    "preservation:image:first",
    "preservation:image:last",
    "preservation:audio",
    "access:audio",
    "access:pdf",
    "preservation:pdf",
    "access:video",
    "preservation:video",
)


class EAD_preprocess:
    """
    Classe de prétraitement EAD Mnesys.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.tree = None
        self.result = None

    def load(self) -> None:
        """Charge le fichier source."""
        self.tree = etree.parse(self.filepath)

    def check_odd_in_c(self, only_with_odd: bool = False) -> list[dict]:
        """
        Parcourt tous les éléments <c> du document EAD.

        Si only_with_odd=True, ne retourne que les <c> possédant un enfant <odd>.
        Retourne une liste de dicts :
          [{"id": str, "level": str, "unitid": str, "has_odd": bool}, ...]
        """
        if self.tree is None:
            raise ValueError("Le fichier n'a pas été chargé. Appelez load() d'abord.")

        root = self.tree.getroot()
        results = []

        for c_elem in root.iter("c"):
            has_odd = c_elem.find("odd") is not None
            if only_with_odd and not has_odd:
                continue
            results.append({
                "id": c_elem.get("id", ""),
                "level": c_elem.get("level", ""),
                "unitid": (c_elem.findtext("did/unitid") or "").strip(),
                "has_odd": has_odd,
            })

        return results

    def convert_dao_to_daoloc(self) -> int:
        """
        Pour chaque <c> possédant un enfant direct <dao>, convertit cet élément en <daoloc>
        et l'insère dans le <daogrp> du même <c> (créé si absent).
        Retourne le nombre d'éléments convertis.
        """
        if self.tree is None:
            raise ValueError("Le fichier n'a pas été chargé. Appelez load() d'abord.")

        count = 0
        for c_elem in self.tree.getroot().iter("c"):
            dao = c_elem.find("dao")
            if dao is None:
                continue

            daogrp = c_elem.find("daogrp")
            if daogrp is None:
                daogrp = etree.SubElement(c_elem, "daogrp")

            daoloc = etree.SubElement(daogrp, "daoloc")
            daoloc.attrib.update(dao.attrib)

            c_elem.remove(dao)
            count += 1

        return count

    def apply_odd_to_daoloc(self) -> int:
        """
        Pour chaque <p> d'un <odd> commençant par un rôle EAD reconnu (cf. ODD_ROLES,
        « Grammaire des role » de documentation/files/donnees/dao_daogrp.md) suivi d'un
        espace puis d'un nom de fichier, ajoute un lien href="<fichier>" role="<rôle>"
        au <c> parent du <odd> — délègue à dao_ark.add_ark_links (scripts/ead/dao_ark.py),
        partagée avec ead_bnr2mnesys.py et add_dao_ark : <daogrp> déjà présent → nouveau
        <daoloc> dans ce groupe (sans doublon de role) ; <dao> isolé déjà présent → converti
        en <daoloc> dans un nouveau <daogrp> avec les nouveaux liens ; ni l'un ni l'autre →
        nouveau <dao> (lien unique) ou <daogrp> (plusieurs liens).
        Supprime le <odd> traité (si au moins un <p> a été reconnu).
        Retourne le nombre de liens ajoutés.
        """
        if self.tree is None:
            raise ValueError("Le fichier n'a pas été chargé. Appelez load() d'abord.")

        def link_builder(c_elem):
            odd = c_elem.find("odd")
            if odd is None:
                return []

            liens = []
            for p in odd.iter("p"):
                text = (p.text or "").strip()
                for role in ODD_ROLES:
                    if text.startswith(role + " "):
                        filename = text[len(role) + 1:].strip()
                        liens.append((filename, role))
                        break

            if liens:
                c_elem.remove(odd)
            return liens

        return add_ark_links(self.tree.getroot(), link_builder, tags=("c",))

    def add_dao_ark(self) -> int:
        """
        Pour chaque <c> possédant un attribut 'id', ajoute un lien ARK
        (https://www.bn-r.fr/ark:/20179/BNR<id>, role="ark") sous forme de
        <dao>/<daoloc> — cf. dao_ark.add_ark_links (scripts/ead/dao_ark.py),
        partagée avec ead_bnr2mnesys.py : un rôle déjà présent dans un <daogrp>
        existant n'est pas dupliqué, et le lien est inséré avant les <c>/<dsc>
        enfants s'il y en a.
        Retourne le nombre de liens ARK ajoutés.
        """
        if self.tree is None:
            raise ValueError("Le fichier n'a pas été chargé. Appelez load() d'abord.")

        def link_builder(c_elem):
            ark = c_elem.get("id")
            if not ark:
                return []
            return [(f"https://www.bn-r.fr/ark:/20179/BNR{ark}", "ark")]

        return add_ark_links(self.tree.getroot(), link_builder, tags=("c",))

    def transform(self, progress_callback=None) -> None:
        """
        Applique les transformations EAD de pré-traitement.
        progress_callback(value: int, message: str) permet de mettre à jour l'UI.
        """
        if self.tree is None:
            raise ValueError("Le fichier n'a pas été chargé. Appelez load() d'abord.")

        if progress_callback:
            progress_callback(25, "Analyse de la structure EAD…")

        self.convert_dao_to_daoloc()
        self.apply_odd_to_daoloc()
        self.add_dao_ark()

        if progress_callback:
            progress_callback(60, "Application des règles de conversion…")

        docinfo = self.tree.docinfo
        self.result = etree.tostring(
            self.tree,
            encoding=docinfo.encoding or "UTF-8",
            xml_declaration=True,
            doctype=docinfo.doctype or None,
        )

        if progress_callback:
            progress_callback(90, "Finalisation du document…")

    def save(self, output_path: str) -> None:
        """Enregistre le fichier transformé."""
        if self.result is None:
            raise ValueError("Aucun résultat à sauvegarder. Appelez transform() d'abord.")
        with open(output_path, "wb") as f:
            f.write(self.result)

    @property
    def output_filename(self) -> str:
        """Suggère un nom de fichier de sortie."""
        name, ext = os.path.splitext(self.filename)
        return f"{name}_mnesys{ext}"
