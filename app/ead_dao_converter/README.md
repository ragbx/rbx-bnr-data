# EAD

Application Python avec interface graphique pour maintenir à jour les `<dao>`/`<daogrp>`
d'un fichier EAD à partir de son `<odd>`, considéré comme la donnée maître.
Destinée aux fichiers de `results/ead/ead_cor/bnr2mnesys/` (sortie de
`ead_bnr2mnesys.py`) : quand un `<odd>` y est corrigé à la main (ajout, modification
ou suppression d'un lien), cette app répercute la correction sur les
`<dao>`/`<daoloc>` correspondants.

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

La logique métier se trouve dans `ead_preprocess.py`, classe `EAD_preprocess`, et
tient en une seule opération :

- `sync_dao_from_odd()` — pour chaque `<c>` possédant un `<odd>`, apparie ses `<p>`
  reconnus (format `role href [audience]`, rôle EAD parmi `ODD_ROLES`, grammaire
  documentée dans
  [Les liens DAO : structures et cas de figure](../../documentation/files/donnees/dao_daogrp.md#le-résumé-odd-donnée-maître))
  aux `<dao>`/`<daoloc>` existants **par `href`** :
  - `href` déjà présent → `role`/`audience` mis à jour si besoin ;
  - `href` absent → nouveau `<daoloc>`/`<dao>` créé (`<daogrp>` existant, `<dao>`
    isolé converti en `<daogrp>`, ou nouveau `<dao>`/`<daogrp>` si aucun des deux
    n'existe encore — mécanique de `dao_ark.add_ark_links`) ;
  - `<dao>`/`<daoloc>` existant dont le `href` a disparu du `<odd>` → supprimé.

  Les `<p>` qui ne commencent par aucun rôle reconnu sont ignorés (notes
  éditoriales éventuelles du `<odd>`, sans rapport avec les dao). Le `<odd>`
  lui-même n'est jamais modifié ni supprimé : il reste la référence pour les
  exécutions suivantes. Retourne `{"ajoutes": int, "modifies": int, "supprimes": int}`.

`transform(progress_callback)` appelle cette méthode et sérialise le résultat ;
c'est ce qu'utilise l'interface graphique.

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
