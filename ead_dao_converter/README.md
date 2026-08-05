# EAD 

Application Python avec interface graphique pour pré-traiter des fichiers EAD avant publication (en particulier, insertion des dao).

## Structure

```
├── app.py              # Interface graphique (Tkinter)
├── ead_preprocess.py   # Classe EAD_preprocess (transformations)
├── img/                # Ressources graphiques (logo)
├── build/               # Spec PyInstaller + mode d'emploi Windows
├── requirements.txt
└── README.md
```

`ead_preprocess.py` importe `dao_ark` (fusion des `daogrp`, insertion des liens ARK),
un module partagé avec `scripts/ead/ead_bnr2mnesys.py` situé dans
`scripts/ead/dao_ark.py` à la racine du dépôt. L'application doit donc rester dans
un dépôt complet (pas de copie isolée du seul dossier `ead_dao_converter/`).

## Lancement

```bash
python app.py
```

## Dépendances

- **Runtime** : Tkinter (bibliothèque standard) + `lxml` (voir `requirements.txt`, déjà fournie par l'environnement conda `rbx-bnr-data` du dépôt)
- **Packaging** : `pyinstaller` (voir `requirements.txt`) — uniquement pour générer un exécutable autonome

## Transformations appliquées

La logique métier se trouve dans `ead_preprocess.py`, classe `EAD_preprocess` :

1. `convert_dao_to_daoloc()` — convertit les éléments `<dao>` en `<daoloc>` dans un `<daogrp>`
2. `apply_odd_to_daoloc()` — transfère les métadonnées `<odd>` vers les attributs `href` des `<daoloc>`
3. `add_dao_ark()` — ajoute un lien ARK (`https://www.bn-r.fr/ark:/20179/BNR{id}`) pour chaque `<c>` identifié.

Ces trois étapes sont enchaînées par `transform(progress_callback)`.

## Exécutable Windows (sans installer Python)

Voir **`build/BUILD_WINDOWS.md`**. En résumé, sur une machine Windows :

```bat
cd ead_dao_converter
pip install -r requirements.txt pyinstaller
pyinstaller build\ead_dao_converter.spec
```

→ `dist\ConvertisseurEAD.exe` (autonome). Un workflow GitHub Actions
(`.github/workflows/build-windows-dao-converter.yml`) produit aussi automatiquement
le `.exe` en artefact téléchargeable à chaque modification de `ead_dao_converter/`.

## Fonctionnalités UI

- Sélection du fichier source via explorateur de fichiers
- Suggestion automatique du nom de fichier de sortie
- Barre de progression animée
- Journal des opérations en temps réel
- Traitement en thread secondaire (UI non bloquée)
