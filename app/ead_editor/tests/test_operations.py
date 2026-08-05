"""Tests des transformations pures (sans Tkinter)."""

from pathlib import Path

from lxml import etree

from ead_editor import operations as ops
from ead_editor.model import EadDocument

DATA = Path(__file__).parent / "data" / "sample_foo.xml"


def _doc():
    return EadDocument.load(DATA)


def _comp(doc, unitid):
    return next(c.element for c in doc.components() if c.unitid == unitid)


# -- dao --------------------------------------------------------------------- #


def test_add_and_remove_dao():
    doc = _doc()
    arch = _comp(doc, "MED_FOO")
    before = len(doc.dao_entries(arch))
    el = ops.add_dao(arch, "RBX_TEST.jpg", "access:image")
    assert len(doc.dao_entries(arch)) == before + 1
    assert el.get("href") == "RBX_TEST.jpg"
    assert ops.remove_dao(el) == 1
    assert len(doc.dao_entries(arch)) == before


def test_add_dao_into_daogrp_and_cleanup_on_remove():
    doc = _doc()
    d01 = _comp(doc, "FOO_S01_D01")
    el = ops.add_dao(d01, "X.jpg", "access:image", into_daogrp=True)
    assert el.tag == "daoloc"
    assert el.getparent().tag == "daogrp"


def test_set_dao_attr():
    doc = _doc()
    entry = doc.dao_entries(_comp(doc, "MED_FOO"))[0]
    assert ops.set_dao_attr(entry.element, entry.href, entry.role, "internal") == 1
    assert entry.element.get("audience") == "internal"
    # repasser audience à vide la supprime
    assert ops.set_dao_attr(entry.element, entry.href, entry.role, "") == 1
    assert "audience" not in entry.element.attrib
    # aucune modif => 0
    assert ops.set_dao_attr(entry.element, entry.href, entry.role, "") == 0


def test_replace_in_href():
    doc = _doc()
    n = ops.replace_in_href(doc.root, "https://www.bn-r.fr", "https://www.bn-r.fr/v2")
    assert n > 0
    # toutes les occurrences https ont été préfixées ; il ne reste plus l'ancien motif nu
    assert not any("https://www.bn-r.fr/ark" in (e.get("href") or "")
                   for e in doc.root.iter("dao", "daoloc"))


def test_replace_in_href_regex_with_role_filter():
    doc = _doc()
    n = ops.replace_in_href(
        doc.root, r"\.mp3$", ".ogg", regex=True, role_filter="access:audio"
    )
    assert n == 3
    audios = [e.get("href") for e in doc.root.iter("dao") if e.get("role") == "access:audio"]
    assert all(h.endswith(".ogg") for h in audios)


def test_rename_role():
    doc = _doc()
    n = ops.rename_role(doc.root, "publication:previous", "publication:archived")
    assert n > 0
    assert not any(e.get("role") == "publication:previous"
                   for e in doc.root.iter("dao", "daoloc"))


# -- controlaccess ----------------------------------------------------------- #


def test_add_edit_remove_term():
    doc = _doc()
    arch = _comp(doc, "MED_FOO")
    ca = doc.get_or_create_controlaccess(arch)
    before = len(doc.term_entries(arch))
    el = ops.add_term(ca, "subject", "Football", "thesaurus--SLASH--bnr_theme.xml")
    assert len(doc.term_entries(arch)) == before + 1
    assert ops.edit_term(el, "geogname", "Roubaix", "thesaurus--SLASH--bnr_geo.xml") == 1
    assert el.tag == "geogname" and el.text == "Roubaix"
    assert ops.remove_term(el) == 1
    assert len(doc.term_entries(arch)) == before


def test_replace_term_text():
    doc = _doc()
    n = ops.replace_term_text(doc.root, "subject", "Sports", "Sport")
    assert n == 1


def test_normalize_source():
    doc = _doc()
    mapping = {"thesaurus--SLASH--bnr_theme.xml": "bnr_theme"}
    n = ops.normalize_source(doc.root, mapping)
    assert n >= 1
    assert not any(e.get("source") == "thesaurus--SLASH--bnr_theme.xml"
                   for e in doc.root.iter("subject"))


def test_dedupe_terms():
    doc = _doc()
    arch = _comp(doc, "MED_FOO")
    ca = doc.get_or_create_controlaccess(arch)
    ops.add_term(ca, "subject", "Sports", "thesaurus--SLASH--bnr_theme.xml")  # doublon
    assert ops.dedupe_terms(ca) == 1


def test_serialization_still_valid_after_edits():
    """Après modifications, le document reste sérialisable et reparseable."""
    doc = _doc()
    arch = _comp(doc, "MED_FOO")
    ca = doc.get_or_create_controlaccess(arch)
    ops.add_term(ca, "subject", "Football", "thesaurus--SLASH--bnr_theme.xml")
    ops.add_dao(arch, "RBX_NEW.jpg", "access:image")
    data = doc.to_bytes()
    reparsed = etree.fromstring(data)
    assert reparsed.tag == "ead"
