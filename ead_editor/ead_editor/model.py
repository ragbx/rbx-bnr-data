"""Modèle de document EAD : entrées/sorties lxml et accès aux dao / controlaccess.

Le modèle est volontairement sans dépendance à Tkinter pour pouvoir être testé
en mode headless et réutilisé par le traitement par lot.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from lxml import etree

# --------------------------------------------------------------------------- #
# Constantes EAD
# --------------------------------------------------------------------------- #

#: Balises usuelles que l'on peut trouver sous <controlaccess>.
CONTROLACCESS_TAGS = (
    "subject",
    "geogname",
    "persname",
    "corpname",
    "famname",
    "genreform",
    "function",
    "occupation",
    "title",
    "name",
)

#: Éléments décrivant une numérisation (objet numérique associé).
DAO_TAGS = ("dao", "daoloc")

#: Éléments devant lesquels insérer un <controlaccess> (ordre DTD EAD).
_TRAILING_TAGS = ("dao", "daogrp", "dsc", "c")


# --------------------------------------------------------------------------- #
# Petites structures de lecture (immuables) renvoyées vers l'UI
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ComponentInfo:
    """Une ligne du navigateur : un <archdesc> ou un <c>."""

    element: etree._Element
    tag: str          # "archdesc" ou "c"
    level: str        # valeur de l'attribut @level
    unitid: str
    unittitle: str
    depth: int        # profondeur d'affichage (archdesc = 0)


@dataclass(frozen=True)
class DaoEntry:
    """Une numérisation : <dao> isolé ou <daoloc> dans un <daogrp>."""

    element: etree._Element
    kind: str         # "dao" ou "daoloc"
    href: str
    role: str
    audience: str


@dataclass(frozen=True)
class TermEntry:
    """Un terme d'indexation, enfant direct de <controlaccess>."""

    element: etree._Element
    tag: str          # subject, geogname, persname, ...
    text: str
    source: str


# --------------------------------------------------------------------------- #
# Helpers d'indentation : préserver la mise en forme du fichier
# --------------------------------------------------------------------------- #


def append_child_indented(parent: etree._Element, child: etree._Element) -> None:
    """Ajoute ``child`` à la fin de ``parent`` en respectant l'indentation existante."""
    children = list(parent)
    if children:
        last = children[-1]
        if len(children) >= 2 and children[-2].tail is not None:
            inter = children[-2].tail
        elif parent.text is not None:
            inter = parent.text
        else:
            inter = "\n"
        child.tail = last.tail        # reprend l'indentation de fermeture
        last.tail = inter             # rétablit l'indentation inter-éléments
    else:
        base = parent.text if parent.text and parent.text.strip() == "" else "\n"
        child.tail = base
        parent.text = base + "  "
    parent.append(child)


def insert_child_indented(
    parent: etree._Element, child: etree._Element, before: etree._Element
) -> None:
    """Insère ``child`` juste avant ``before`` en respectant l'indentation."""
    idx = list(parent).index(before)
    if idx == 0:
        # devient le premier enfant : reprend le tail de l'ancien premier
        child.tail = before.tail if parent.text is None else parent.text
    else:
        prev = list(parent)[idx - 1]
        child.tail = prev.tail
    parent.insert(idx, child)


def remove_child_indented(child: etree._Element) -> None:
    """Supprime ``child`` en réparant l'indentation des frères restants."""
    parent = child.getparent()
    if parent is None:
        return
    children = list(parent)
    idx = children.index(child)
    if idx == len(children) - 1 and idx > 0:
        children[idx - 1].tail = child.tail
    parent.remove(child)


# --------------------------------------------------------------------------- #
# Document EAD
# --------------------------------------------------------------------------- #


class EadDocument:
    """Charge, expose et enregistre un fichier EAD."""

    def __init__(
        self,
        tree: etree._ElementTree,
        path: Path | None = None,
        final_newline: bool = True,
    ) -> None:
        self._tree = tree
        self._root = tree.getroot()
        self.path: Path | None = path
        self.dirty: bool = False
        # Préserve la présence (ou non) d'un saut de ligne final dans le fichier source.
        self._final_newline = final_newline

    # -- chargement / enregistrement -------------------------------------- #

    @classmethod
    def _parser(cls) -> etree.XMLParser:
        # resolve_entities=False : conserve le HTML échappé tel quel.
        # load_dtd / no_network=True : pas d'accès réseau à la DTD ead2002.
        return etree.XMLParser(
            resolve_entities=False,
            load_dtd=False,
            no_network=True,
            remove_blank_text=False,
        )

    @classmethod
    def load(cls, path: str | Path) -> "EadDocument":
        path = Path(path)
        raw = path.read_bytes()
        tree = etree.parse(str(path), cls._parser())
        return cls(tree, path, final_newline=raw.endswith(b"\n"))

    @classmethod
    def from_string(cls, data: bytes | str, path: Path | None = None) -> "EadDocument":
        if isinstance(data, str):
            data = data.encode("utf-8")
        tree = etree.ElementTree(etree.fromstring(data, cls._parser()))
        return cls(tree, path, final_newline=data.endswith(b"\n"))

    def to_bytes(self) -> bytes:
        """Sérialise en préservant la déclaration XML, le DOCTYPE et le saut final."""
        doctype = self._tree.docinfo.doctype or None
        data = etree.tostring(
            self._tree,
            encoding="UTF-8",
            xml_declaration=True,
            doctype=doctype,
        )
        if self._final_newline and not data.endswith(b"\n"):
            data += b"\n"
        return data

    def save(self, path: str | Path | None = None, backup: bool = True) -> Path:
        target = Path(path) if path is not None else self.path
        if target is None:
            raise ValueError("Aucun chemin de destination fourni.")
        if backup and target.exists():
            bak = target.with_suffix(target.suffix + ".bak")
            bak.write_bytes(target.read_bytes())
        target.write_bytes(self.to_bytes())
        self.path = target
        self.dirty = False
        return target

    def mark_dirty(self) -> None:
        self.dirty = True

    # -- navigation : composants ------------------------------------------ #

    @property
    def root(self) -> etree._Element:
        return self._root

    def _did_text(self, component: etree._Element, child_tag: str) -> str:
        did = component.find("did")
        if did is None:
            return ""
        el = did.find(child_tag)
        return (el.text or "").strip() if el is not None else ""

    def components(self) -> list[ComponentInfo]:
        """Liste à plat de <archdesc> puis de tous les <c> (récursif, en profondeur)."""
        result: list[ComponentInfo] = []
        archdesc = self._root.find("archdesc")
        if archdesc is None:
            return result
        result.append(
            ComponentInfo(
                element=archdesc,
                tag="archdesc",
                level=archdesc.get("level", ""),
                unitid=self._did_text(archdesc, "unitid"),
                unittitle=self._did_text(archdesc, "unittitle"),
                depth=0,
            )
        )

        def walk(parent: etree._Element, depth: int) -> None:
            for c in parent.findall("c"):
                result.append(
                    ComponentInfo(
                        element=c,
                        tag="c",
                        level=c.get("level", ""),
                        unitid=self._did_text(c, "unitid"),
                        unittitle=self._did_text(c, "unittitle"),
                        depth=depth,
                    )
                )
                walk(c, depth + 1)

        dsc = archdesc.find("dsc")
        if dsc is not None:
            walk(dsc, 1)
        # composants <c> imbriqués directement (corpus tolérant)
        walk(archdesc, 1)
        return result

    # -- numérisations (dao) ---------------------------------------------- #

    def dao_entries(self, component: etree._Element) -> list[DaoEntry]:
        """dao isolés + daoloc des daogrp directs du composant, en ordre document."""
        entries: list[DaoEntry] = []
        for child in component:
            if child.tag == "dao":
                entries.append(self._dao_entry(child, "dao"))
            elif child.tag == "daogrp":
                for loc in child:
                    if loc.tag == "daoloc":
                        entries.append(self._dao_entry(loc, "daoloc"))
        return entries

    @staticmethod
    def _dao_entry(el: etree._Element, kind: str) -> DaoEntry:
        return DaoEntry(
            element=el,
            kind=kind,
            href=el.get("href", ""),
            role=el.get("role", ""),
            audience=el.get("audience", ""),
        )

    def known_roles(self) -> list[str]:
        roles = {
            el.get("role", "")
            for el in self._root.iter(*DAO_TAGS)
            if el.get("role")
        }
        return sorted(roles)

    # -- indexation (controlaccess) --------------------------------------- #

    def controlaccess(self, component: etree._Element) -> etree._Element | None:
        return component.find("controlaccess")

    def term_entries(self, component: etree._Element) -> list[TermEntry]:
        ca = self.controlaccess(component)
        if ca is None:
            return []
        entries: list[TermEntry] = []
        for el in ca:
            if not isinstance(el.tag, str):  # commentaires éventuels
                continue
            entries.append(
                TermEntry(
                    element=el,
                    tag=el.tag,
                    text=(el.text or "").strip(),
                    source=el.get("source", ""),
                )
            )
        return entries

    def get_or_create_controlaccess(self, component: etree._Element) -> etree._Element:
        ca = component.find("controlaccess")
        if ca is not None:
            return ca
        ca = etree.SubElement(component, "controlaccess")  # placeholder, repositionné
        component.remove(ca)
        # insérer avant le premier élément "de fin" (dao/daogrp/dsc) s'il existe
        before = None
        for child in component:
            if child.tag in _TRAILING_TAGS:
                before = child
                break
        if before is not None:
            insert_child_indented(component, ca, before)
        else:
            append_child_indented(component, ca)
        return ca

    def known_sources(self) -> list[str]:
        sources = {
            el.get("source", "")
            for el in self._root.iter(*CONTROLACCESS_TAGS)
            if el.get("source")
        }
        return sorted(sources)


# --------------------------------------------------------------------------- #
# Utilitaires
# --------------------------------------------------------------------------- #


def is_ead_file(path: str | Path) -> bool:
    """Heuristique légère : le fichier commence-t-il par un EAD ?"""
    path = Path(path)
    if path.suffix.lower() != ".xml":
        return False
    try:
        head = path.read_bytes()[:4096].decode("utf-8", errors="ignore")
    except OSError:
        return False
    return bool(re.search(r"<!DOCTYPE\s+ead|<ead[\s>]", head))
