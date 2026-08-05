# -*- mode: python ; coding: utf-8 -*-
"""Spec PyInstaller pour produire EditeurEAD.exe (Windows, autonome).

Génération :
    pyinstaller build/ead_editor.spec
Résultat :
    dist/EditeurEAD.exe   (aucune installation Python requise sur la cible)
"""

import sys
from pathlib import Path

# Le .spec est exécuté depuis la racine du projet (dossier ead_editor/).
PROJECT_ROOT = Path(SPECPATH).resolve().parent
ENTRY = str(PROJECT_ROOT / "ead_editor" / "__main__.py")

block_cipher = None

a = Analysis(
    [ENTRY],
    pathex=[str(PROJECT_ROOT)],
    binaries=[],
    datas=[],
    hiddenimports=["lxml._elementpath"],
    hookspath=[],
    runtime_hooks=[],
    excludes=["numpy", "pandas", "matplotlib", "PIL", "pytest"],
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="EditeurEAD",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    runtime_tmpdir=None,
    console=False,        # application graphique : pas de console
    disable_windowed_traceback=False,
    icon=None,
)
