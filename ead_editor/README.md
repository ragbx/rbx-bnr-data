# Éditeur EAD BnR

Application de bureau (Python + Tkinter) pour **corriger et transformer** les fichiers
XML EAD de la Bibliothèque numérique de Roubaix, avec une attention particulière aux
balises :

- **`dao` / `daoloc`** — numérisations (`href`, `role`, `audience`) ;
- **`controlaccess`** — indexation (`subject`, `geogname`, `persname`, `corpname`,
  `genreform` + attribut `source`).

L'édition se fait par **formulaires guidés** (pas de XML brut), le DOCTYPE et la mise en
forme du fichier source sont préservés.

## Fonctionnalités

- Navigation dans l'arborescence de l'IR (`archdesc` + composants `c`).
- Édition unitaire des numérisations et des termes d'indexation du composant sélectionné
  (ajouter / modifier / dupliquer / supprimer, dédoublonnage de l'indexation).
- **Traitement par lot** sur un dossier : renommer un rôle, remplacer une portion de
  `href` (avec option *regex* et filtre par rôle), remplacer un terme, normaliser une
  `source` — avec **aperçu sans écriture** puis application avec sauvegarde `.bak`.
- Sauvegarde `.bak` automatique avant tout écrasement.

## Lancer depuis les sources (développement)

```bash
# avec l'environnement conda du projet
conda run -n ds python -m ead_editor
# ou
pip install -r requirements.txt
python -m ead_editor
```

## Tests

```bash
cd ead_editor
python -m pytest -q          # ou : conda run -n ds python -m pytest -q
```

## Exécutable Windows (sans installer Python)

Voir **`build/BUILD_WINDOWS.md`**. En résumé, sur une machine Windows :

```bat
pip install -r requirements.txt pyinstaller
pyinstaller build\ead_editor.spec
```

→ `dist\EditeurEAD.exe` (autonome). Un workflow GitHub Actions
(`.github/workflows/build-windows.yml`) produit aussi automatiquement le `.exe` en
artefact téléchargeable.

## Structure

```
ead_editor/
  ead_editor/
    model.py            # EadDocument : I/O lxml, accès dao/controlaccess (sans Tk)
    operations.py       # transformations pures, réutilisées par l'UI et le lot
    ui/                 # interface Tkinter (app, navigateur, panneaux, lot, dialogues)
  tests/                # tests headless (model + operations)
  build/                # spec PyInstaller + mode d'emploi Windows
```
