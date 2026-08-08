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
2. `apply_odd_to_daoloc()` — pour chaque `<p>` d'un `<odd>` commençant par un rôle EAD
   reconnu (cf. `ODD_ROLES`, grammaire documentée dans
   `documentation/files/donnees/dao_daogrp.md`) suivi d'un espace puis d'un nom de
   fichier, ajoute le lien au `<c>` parent : nouveau `<daoloc>` dans le `<daogrp>`
   existant, ou `<dao>` isolé existant converti en `<daoloc>` dans un nouveau
   `<daogrp>`, ou nouveau `<dao>`/`<daogrp>` si aucun des deux n'existe encore.
   Consomme le `<odd>` (le supprime une fois ses `<p>` reconnus traités).
3. `add_dao_ark()` — ajoute un lien ARK (`https://www.bn-r.fr/ark:/20179/BNR{id}`) pour chaque `<c>` identifié.

Ces trois étapes sont enchaînées par `transform(progress_callback)`.

### Cas particulier : fichiers `results/ead/ead_cor/bnr2mnesys/`

Dans ces fichiers (déjà transformés par `ead_bnr2mnesys.py`, étape 10), le `<odd>`
de chaque `<c>` résume déjà ses liens dao/daoloc en clair et est considéré comme
**la donnée maître** — cf.
[Les liens DAO : structures et cas de figure](../../documentation/files/donnees/dao_daogrp.md#le-résumé-odd-donnée-maître).
Pour répercuter une correction faite à la main dans un `<odd>` (ajout,
modification, suppression d'un `<p>`) sur les `<dao>`/`<daoloc>` correspondants,
utiliser plutôt :

- `sync_dao_from_odd()` — pour chaque `<c>` possédant un `<odd>`, apparie ses `<p>`
  (format `role href [audience]`) aux `<dao>`/`<daoloc>` existants **par `href`** :
  mise à jour du `role`/`audience` si le `href` existe déjà, création sinon (même
  mécanique d'insertion que `apply_odd_to_daoloc`), suppression du `<dao>`/`<daoloc>`
  dont le `href` a disparu du `<odd>`. Le `<odd>` n'est jamais modifié ni supprimé :
  il reste réutilisable aux exécutions suivantes. Retourne
  `{"ajoutes": int, "modifies": int, "supprimes": int}`.

Cette méthode n'est pas enchaînée par `transform()` (qui vise le pré-traitement
avant première publication, cas différent) : elle s'utilise séparément, sur un
fichier déjà présent dans `results/ead/ead_cor/bnr2mnesys/`.

## Exécutable Windows (sans installer Python)

Voir **`build/BUILD_WINDOWS.md`**. En résumé, sur une machine Windows :

```bat
cd app/ead_dao_converter
pip install -r requirements.txt pyinstaller
pyinstaller build\ead_dao_converter.spec --distpath ..\dist
```

→ `app\dist\ConvertisseurEAD.exe` (autonome). Un workflow GitHub Actions
(`.github/workflows/build-windows-dao-converter.yml`) reconstruit aussi
automatiquement le `.exe` à chaque modification de `app/ead_dao_converter/`,
disponible à deux endroits :

- **Lien stable** (toujours le dernier build réussi) :
  https://github.com/ragbx/rbx-bnr-data/releases/download/build-ead-dao-converter/ConvertisseurEAD.exe
- Artefact du run CI (onglet *Actions*, section *Artifacts* du run) — rétention limitée.

## Fonctionnalités UI

- Sélection du fichier source via explorateur de fichiers
- Suggestion automatique du nom de fichier de sortie
- Barre de progression animée
- Journal des opérations en temps réel
- Traitement en thread secondaire (UI non bloquée)
