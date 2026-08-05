# -*- mode: python ; coding: utf-8 -*-
"""Spec PyInstaller pour produire ConvertisseurEAD.exe (Windows, autonome).

Génération :
    pyinstaller build/ead_dao_converter.spec
Résultat :
    dist/ConvertisseurEAD.exe   (aucune installation Python requise sur la cible)
"""

from pathlib import Path

# Le .spec est exécuté depuis la racine du projet (dossier app/ead_dao_converter/).
PROJECT_ROOT = Path(SPECPATH).resolve().parent
ENTRY = str(PROJECT_ROOT / "app.py")

# ead_preprocess.py importe dao_ark (scripts/ead/dao_ark.py, module partagé avec
# ead_bnr2mnesys.py) via un sys.path.insert basé sur son propre chemin ; il faut
# aussi le déclarer ici pour que l'analyse statique de PyInstaller le trouve et
# l'embarque dans l'exécutable.
# PROJECT_ROOT.parent.parent : app/ead_dao_converter -> app -> racine du dépôt.
SCRIPTS_EAD = PROJECT_ROOT.parent.parent / "scripts" / "ead"

block_cipher = None

a = Analysis(
    [ENTRY],
    pathex=[str(PROJECT_ROOT), str(SCRIPTS_EAD)],
    binaries=[],
    datas=[(str(PROJECT_ROOT / "img" / "logo.png"), "img")],
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
    name="ConvertisseurEAD",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    runtime_tmpdir=None,
    console=False,        # application graphique : pas de console
    disable_windowed_traceback=False,
    icon=None,
)
