"""
Classe EAD_preprocess : opérations de préparation d'un EAD Mnesys en vue de sa
publication. Pour l'instant, une seule opération est implémentée
(sync_dao_from_odd, synchronisation des <dao>/<daogrp> à partir des <odd>) ;
d'autres pourront s'y ajouter par la suite sans changer cette classe d'accueil.
"""

import os
import sys
from pathlib import Path

from lxml import etree

# Réutilise la mécanique d'insertion des liens ARK partagée avec
# scripts/ead/ead_bnr2mnesys.py (dedup, cf. scripts/ead/dao_ark.py).
# parents[2] : app/ead_prepublication/ead_preprocess.py -> app/ead_prepublication -> app -> racine du dépôt.
_SCRIPTS_EAD = Path(__file__).resolve().parents[2] / "scripts" / "ead"
if str(_SCRIPTS_EAD) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_EAD))

from dao_ark import add_ark_links  # noqa: E402

# Rôles EAD reconnus dans les <p> d'un <odd> (cf. sync_dao_from_odd), tels que
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
    "access:ocr_alto",
    "preservation:ocr_alto",
)

# Valeurs reconnues de l'attribut audience (cf. documentation/files/donnees/dao_daogrp.md :
# "internal" est la seule valeur observée dans tout le corpus). Utilisé pour distinguer,
# dans un <p> "role href [audience]" de sync_dao_from_odd, l'audience d'un href qui
# contiendrait lui-même un espace (cas réel : noms de fichiers mal saisis).
ODD_AUDIENCES = ("internal",)


class EAD_preprocess:
    """
    Classe de préparation d'un EAD Mnesys en vue de sa publication. Aujourd'hui,
    ne traite que les <dao>/<daoloc> : synchronisation à partir des <odd> (donnée
    maître) pour les fichiers de results/ead/ead_cor/bnr2mnesys/, cf. sync_dao_from_odd.
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

    def sync_dao_from_odd(self) -> dict:
        """
        Pour chaque <c> possédant un <odd>, synchronise ses <dao>/<daoloc> pour qu'ils
        reflètent exactement les <p> reconnus du <odd> (l'<odd> est la donnée maître) :

        - chaque <p> commençant par un rôle EAD reconnu (cf. ODD_ROLES) suivi d'un
          espace, d'un href puis, optionnellement, d'un espace et d'une audience
          ("role href" ou "role href audience") ;
        - les <p> qui ne commencent par aucun rôle reconnu sont ignorés (notes
          éditoriales éventuelles du <odd>, non liées aux dao) ;
        - un <dao>/<daoloc> existant est apparié à un <p> par le triplet complet
          (href, role, audience) — pas par le href seul, qui peut légitimement se
          répéter dans un même <daogrp> sous des role différents (ex. un même pdf en
          preservation:pdf et access:pdf, cf. documentation/files/donnees/dao_daogrp.md) ;
        - un triplet présent dans le <odd> mais sans <dao>/<daoloc> correspondant est
          créé : directement dans le <daogrp> s'il existe déjà (pas via
          dao_ark.add_ark_links, dont la dédup par role seul rejetterait à tort un
          role déjà présent sous un autre href — ex. deux pistes access:audio d'un
          même fonds sonore) ; sinon délègue à add_ark_links (<dao> isolé converti
          en <daogrp>, ou nouveau <dao>/<daogrp>) ;
        - un <dao>/<daoloc> existant dont le triplet n'apparaît plus dans le <odd> est
          supprimé. Un simple changement de role/audience sur un href se traduit donc
          par une suppression de l'ancien triplet et un ajout du nouveau (pas de
          modification en place).

        Le <odd> lui-même n'est jamais modifié ni supprimé : il reste la référence
        pour les exécutions suivantes.

        Retourne {"ajoutes": int, "supprimes": int}.
        """
        if self.tree is None:
            raise ValueError("Le fichier n'a pas été chargé. Appelez load() d'abord.")

        def cle(e):
            return (e.get("href"), e.get("role"), e.get("audience"))

        stats = {"ajoutes": 0, "supprimes": 0}

        for c_elem in self.tree.getroot().iter("c"):
            odd = c_elem.find("odd")
            if odd is None:
                continue

            odd_cles = set()
            for p in odd.iter("p"):
                text = (p.text or "").strip()
                for role in ODD_ROLES:
                    if not text.startswith(role + " "):
                        continue
                    reste = text[len(role) + 1:].strip()
                    audience = None
                    for valeur in ODD_AUDIENCES:
                        if reste.endswith(" " + valeur) and len(reste) > len(valeur) + 1:
                            audience = valeur
                            reste = reste[: -(len(valeur) + 1)]
                            break
                    href = reste.strip()
                    if href:
                        odd_cles.add((href, role, audience))
                    break

            existants = [child for child in c_elem if child.tag == "dao"]
            for daogrp in c_elem.findall("daogrp"):
                existants.extend(daogrp.findall("daoloc"))
            existants_cles = {cle(e) for e in existants}

            for elem in existants:
                if cle(elem) not in odd_cles:
                    elem.getparent().remove(elem)
                    stats["supprimes"] += 1

            a_ajouter = odd_cles - existants_cles
            if not a_ajouter:
                continue

            daogrp = c_elem.find("daogrp")
            if daogrp is not None:
                # Insertion directe : a_ajouter ne contient déjà que des triplets
                # absents, donc pas besoin (et pas de risque) de passer par la
                # dédup par role seul de add_ark_links.
                for href, role, audience in a_ajouter:
                    new_daoloc = etree.SubElement(daogrp, "daoloc")
                    new_daoloc.set("href", href)
                    new_daoloc.set("role", role)
                    if audience:
                        new_daoloc.set("audience", audience)
                    stats["ajoutes"] += 1
            else:
                # Ni <dao> isolé ni <daogrp> : ces cas de add_ark_links ajoutent
                # systématiquement tous les liens, donc aucun risque de dédup indue.
                stats["ajoutes"] += add_ark_links(
                    c_elem,
                    lambda el, liens=a_ajouter: [(href, role) for href, role, _ in liens],
                    tags=("c",),
                )
                for href, role, audience in a_ajouter:
                    if not audience:
                        continue
                    for elem in c_elem.iter("dao", "daoloc"):
                        if elem.get("href") == href and elem.get("role") == role and not elem.get("audience"):
                            elem.set("audience", audience)
                            break

        return stats

    def transform(self, progress_callback=None) -> dict:
        """
        Synchronise les <dao>/<daoloc> à partir des <odd> (cf. sync_dao_from_odd).
        progress_callback(value: int, message: str) permet de mettre à jour l'UI.
        Retourne les statistiques de sync_dao_from_odd.
        """
        if self.tree is None:
            raise ValueError("Le fichier n'a pas été chargé. Appelez load() d'abord.")

        if progress_callback:
            progress_callback(25, "Analyse des <odd>…")

        stats = self.sync_dao_from_odd()

        if progress_callback:
            progress_callback(60, "Synchronisation des <dao>/<daoloc>…")

        docinfo = self.tree.docinfo
        self.result = etree.tostring(
            self.tree,
            encoding=docinfo.encoding or "UTF-8",
            xml_declaration=True,
            doctype=docinfo.doctype or None,
        )

        if progress_callback:
            progress_callback(90, "Finalisation du document…")

        return stats

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
        return f"{name}_sync{ext}"
