"""Tests du modèle EAD (sans Tkinter)."""

from pathlib import Path

import pytest

from ead_editor.model import EadDocument, is_ead_file

DATA = Path(__file__).parent / "data" / "sample_foo.xml"


def test_roundtrip_preserves_bytes():
    """Charger puis sérialiser sans modification ne change pas le fichier."""
    orig = DATA.read_bytes()
    doc = EadDocument.load(DATA)
    assert doc.to_bytes() == orig


def test_roundtrip_preserves_doctype_and_declaration():
    out = EadDocument.load(DATA).to_bytes().decode("utf-8")
    assert out.startswith("<?xml version='1.0' encoding='UTF-8'?>")
    assert "<!DOCTYPE ead PUBLIC" in out
    assert 'ead2002/ead.dtd' in out


def test_components_listing():
    doc = EadDocument.load(DATA)
    comps = doc.components()
    assert comps[0].tag == "archdesc"
    assert comps[0].unitid == "MED_FOO"
    tags = [c.tag for c in comps]
    assert tags.count("c") == 4  # 1 série + 3 fichiers
    # le composant série est moins profond que les fichiers
    series = next(c for c in comps if c.unitid == "FOO_S01")
    files = [c for c in comps if c.unitid.startswith("FOO_S01_D")]
    assert all(f.depth > series.depth for f in files)


def test_dao_entries_mixes_dao_and_daoloc():
    doc = EadDocument.load(DATA)
    comps = {c.unitid: c.element for c in doc.components()}
    # l'archdesc a un <dao> isolé
    arch = doc.dao_entries(comps["MED_FOO"])
    assert len(arch) == 1
    assert arch[0].kind == "dao"
    assert arch[0].role == "publication:current"
    # un fichier a un <dao> + un <daogrp> de daoloc
    d01 = doc.dao_entries(comps["FOO_S01_D01"])
    kinds = [e.kind for e in d01]
    assert "dao" in kinds and "daoloc" in kinds


def test_term_entries_and_known_sources():
    doc = EadDocument.load(DATA)
    comps = {c.unitid: c.element for c in doc.components()}
    terms = doc.term_entries(comps["MED_FOO"])
    tags = {t.tag for t in terms}
    assert {"genreform", "subject", "persname"} <= tags
    assert any(t.text == "Sports" for t in terms)
    assert "thesaurus--SLASH--bnr_theme.xml" in doc.known_sources()


def test_get_or_create_controlaccess_existing():
    doc = EadDocument.load(DATA)
    arch = doc.components()[0].element
    ca = doc.get_or_create_controlaccess(arch)
    assert ca is doc.controlaccess(arch)


def test_save_creates_backup(tmp_path):
    target = tmp_path / "f.xml"
    target.write_bytes(DATA.read_bytes())
    doc = EadDocument.load(target)
    doc.save(backup=True)
    assert (tmp_path / "f.xml.bak").exists()


def test_is_ead_file(tmp_path):
    assert is_ead_file(DATA)
    other = tmp_path / "x.xml"
    other.write_text("<html><body/></html>")
    assert not is_ead_file(other)
    assert not is_ead_file(tmp_path / "x.txt")
