# Générer l'exécutable Windows (EditeurEAD.exe)

L'application est livrée sous forme d'un **fichier `.exe` autonome** : l'utilisateur
final n'a **pas besoin d'installer Python**. La génération du `.exe`, elle, doit se
faire **une fois sur une machine Windows** (PyInstaller ne fait pas de compilation
croisée depuis Linux/macOS).

Deux options : build manuel (ci-dessous) ou build automatique via GitHub Actions
(voir `.github/workflows/build-windows.yml`).

## Prérequis (machine Windows)

1. Installer Python 3.10+ (https://www.python.org/downloads/windows/),
   en cochant **« Add Python to PATH »**.
2. Ouvrir un terminal (PowerShell ou CMD) dans le dossier `ead_editor/`.

## Build manuel

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt pyinstaller
pyinstaller build\ead_editor.spec
```

Le binaire est produit dans **`dist\EditeurEAD.exe`**. Il est autonome : on peut le
copier sur n'importe quel poste Windows et le lancer par double-clic.

## Vérification

- Double-cliquer sur `dist\EditeurEAD.exe`.
- *Fichier → Ouvrir…* et charger un fichier EAD (ex. un fichier de
  `results/ead/ead_cor/bnr2mnesys/`).
- Sélectionner un composant dans l'arbre de gauche, éditer un `dao` ou un terme
  `controlaccess`, puis *Fichier → Enregistrer* (un `.bak` est créé).

## Remarques

- `upx=True` dans le `.spec` compresse le binaire si UPX est installé ; sinon
  PyInstaller l'ignore sans erreur.
- Pour ajouter une icône : placer un `.ico` et renseigner `icon="chemin.ico"` dans
  `build/ead_editor.spec`.
- Certains antivirus signalent à tort les binaires PyInstaller (faux positif connu).
