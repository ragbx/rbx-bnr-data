"""Transformations pures sur l'arbre EAD (sans Tkinter).

Toutes les fonctions mutent l'arbre lxml en place et renvoient le nombre de
modifications effectuées, ce qui permet l'aperçu « dry-run » du traitement par lot.
"""

from __future__ import annotations

import re

from lxml import etree

from .model import (
    CONTROLACCESS_TAGS,
    DAO_TAGS,
    append_child_indented,
    insert_child_indented,
    remove_child_indented,
)

# --------------------------------------------------------------------------- #
# Numérisations (dao / daoloc)
# --------------------------------------------------------------------------- #


def _iter_dao(root: etree._Element):
    return (el for el in root.iter(*DAO_TAGS))


def add_dao(
    component: etree._Element,
    href: str,
    role: str,
    audience: str = "",
    *,
    into_daogrp: bool = False,
) -> etree._Element:
    """Ajoute une numérisation au composant.

    Par défaut, ajoute un <dao> isolé. Si ``into_daogrp`` et qu'un <daogrp> existe,
    ajoute plutôt un <daoloc> dans le premier <daogrp>.
    """
    attrs = {"href": href, "role": role}
    if audience:
        attrs["audience"] = audience

    daogrp = component.find("daogrp") if into_daogrp else None
    if daogrp is not None:
        el = etree.Element("daoloc", attrs)
        append_child_indented(daogrp, el)
    else:
        el = etree.Element("dao", attrs)
        # la DTD EAD place dao/daogrp avant les <c> et le <dsc> : insérer avant eux.
        before = next((c for c in component if c.tag in ("dsc", "c")), None)
        if before is not None:
            insert_child_indented(component, el, before)
        else:
            append_child_indented(component, el)
    return el


def remove_dao(element: etree._Element) -> int:
    """Supprime un <dao>/<daoloc>. Supprime aussi un <daogrp> devenu vide."""
    parent = element.getparent()
    if parent is None:
        return 0
    remove_child_indented(element)
    if parent.tag == "daogrp" and len([c for c in parent if isinstance(c.tag, str)]) == 0:
        remove_child_indented(parent)
    return 1


def set_dao_attr(element: etree._Element, href: str, role: str, audience: str = "") -> int:
    """Met à jour href/role/audience d'une numérisation. Renvoie 1 si modifié."""
    changed = False
    if element.get("href", "") != href:
        element.set("href", href)
        changed = True
    if element.get("role", "") != role:
        element.set("role", role)
        changed = True
    current_aud = element.get("audience", "")
    if audience:
        if current_aud != audience:
            element.set("audience", audience)
            changed = True
    elif current_aud:
        del element.attrib["audience"]
        changed = True
    return 1 if changed else 0


def replace_in_href(
    root: etree._Element,
    find: str,
    repl: str,
    *,
    regex: bool = False,
    role_filter: str | None = None,
) -> int:
    """Remplace une portion de @href sur les dao/daoloc. Renvoie le nb de modifs."""
    count = 0
    pattern = re.compile(find) if regex else None
    for el in _iter_dao(root):
        if role_filter and el.get("role", "") != role_filter:
            continue
        href = el.get("href", "")
        new = pattern.sub(repl, href) if regex else href.replace(find, repl)
        if new != href:
            el.set("href", new)
            count += 1
    return count


def rename_role(root: etree._Element, old: str, new: str) -> int:
    """Renomme un rôle de numérisation. Renvoie le nb de dao/daoloc modifiés."""
    count = 0
    for el in _iter_dao(root):
        if el.get("role", "") == old:
            el.set("role", new)
            count += 1
    return count


def set_audience(root: etree._Element, role: str, audience: str) -> int:
    """Force l'attribut audience des dao/daoloc d'un rôle donné (vide => suppression)."""
    count = 0
    for el in _iter_dao(root):
        if role and el.get("role", "") != role:
            continue
        current = el.get("audience", "")
        if audience and current != audience:
            el.set("audience", audience)
            count += 1
        elif not audience and current:
            del el.attrib["audience"]
            count += 1
    return count


# --------------------------------------------------------------------------- #
# Indexation (controlaccess)
# --------------------------------------------------------------------------- #


def add_term(
    controlaccess: etree._Element, tag: str, text: str, source: str = ""
) -> etree._Element:
    """Ajoute un terme (subject, persname, ...) dans un <controlaccess> existant."""
    el = etree.Element(tag)
    el.text = text
    if source:
        el.set("source", source)
    append_child_indented(controlaccess, el)
    return el


def edit_term(element: etree._Element, tag: str, text: str, source: str = "") -> int:
    """Modifie un terme (balise, texte, source). Renvoie 1 si modifié."""
    changed = False
    if element.tag != tag:
        element.tag = tag
        changed = True
    if (element.text or "") != text:
        element.text = text
        changed = True
    current = element.get("source", "")
    if source:
        if current != source:
            element.set("source", source)
            changed = True
    elif current:
        del element.attrib["source"]
        changed = True
    return 1 if changed else 0


def remove_term(element: etree._Element) -> int:
    """Supprime un terme d'indexation. Renvoie 1."""
    remove_child_indented(element)
    return 1


def replace_term_text(root: etree._Element, tag: str | None, old: str, new: str) -> int:
    """Remplace le texte exact ``old`` par ``new`` sur les termes (filtrés par tag)."""
    count = 0
    tags = (tag,) if tag else CONTROLACCESS_TAGS
    for el in root.iter(*tags):
        if (el.text or "").strip() == old:
            el.text = new
            count += 1
    return count


def normalize_source(root: etree._Element, mapping: dict[str, str]) -> int:
    """Remplace les valeurs @source selon ``mapping`` (ancienne -> nouvelle)."""
    count = 0
    for el in root.iter(*CONTROLACCESS_TAGS):
        src = el.get("source", "")
        if src in mapping and mapping[src] != src:
            el.set("source", mapping[src])
            count += 1
    return count


def dedupe_terms(controlaccess: etree._Element) -> int:
    """Supprime les termes en double (même tag, texte et source). Renvoie le nb supprimés."""
    seen: set[tuple[str, str, str]] = set()
    count = 0
    for el in list(controlaccess):
        if not isinstance(el.tag, str):
            continue
        key = (el.tag, (el.text or "").strip(), el.get("source", ""))
        if key in seen:
            remove_child_indented(el)
            count += 1
        else:
            seen.add(key)
    return count
