# Générer l'exécutable Windows (ConvertisseurEAD.exe)

L'application est livrée sous forme d'un **fichier `.exe` autonome** : l'utilisateur
final n'a **pas besoin d'installer Python**. La génération du `.exe`, elle, doit se
faire **une fois sur une machine Windows** (PyInstaller ne fait pas de compilation
croisée depuis Linux/macOS).

Deux options : build manuel (ci-dessous) ou build automatique via GitHub Actions
(voir `.github/workflows/build-windows-dao-converter.yml`).

## Prérequis (machine Windows)

1. Installer Python 3.10+ (https://www.python.org/downloads/windows/),
   en cochant **« Add Python to PATH »**.
2. Ouvrir un terminal (PowerShell ou CMD) **à la racine du dépôt**
   (`app/ead_dao_converter/ead_preprocess.py` importe un module partagé situé dans
   `scripts/ead/`, il faut donc que l'arborescence complète du dépôt soit présente).

## Build manuel

```bat
cd app/ead_dao_converter
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt pyinstaller
pyinstaller build\ead_dao_converter.spec --distpath ..\dist
```

Le binaire est produit dans **`app\dist\ConvertisseurEAD.exe`** (dossier partagé
avec `ead_editor`, cf. `--distpath ..\dist`). Il est autonome : on peut le copier
sur n'importe quel poste Windows et le lancer par double-clic.

## Vérification

- Double-cliquer sur `app\dist\ConvertisseurEAD.exe`.
- Sélectionner un fichier EAD source, vérifier que le nom de sortie est suggéré
  automatiquement, lancer la conversion.
- Vérifier que le logo s'affiche bien dans l'en-tête (il est embarqué dans
  l'exécutable via `datas` dans le `.spec`).

## Remarques

- `upx=True` dans le `.spec` compresse le binaire si UPX est installé ; sinon
  PyInstaller l'ignore sans erreur.
- Le `.spec` déclare `scripts/ead/` dans `pathex` : c'est nécessaire pour que
  PyInstaller embarque `dao_ark.py`, le module partagé avec
  `scripts/ead/ead_bnr2mnesys.py` dont dépend `ead_preprocess.py`.
- Pour ajouter une icône : placer un `.ico` et renseigner `icon="chemin.ico"` dans
  `build/ead_dao_converter.spec`.
- Certains antivirus signalent à tort les binaires PyInstaller (faux positif connu).
