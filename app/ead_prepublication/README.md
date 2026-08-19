# EAD Pré-publication

Application Python avec interface graphique pour préparer les fichiers EAD
Mnesys en vue de leur publication. **Pour l'instant, seule la synchronisation
des `<dao>`/`<daogrp>` est implémentée** : d'autres opérations de préparation
pourront s'y ajouter par la suite, sans changer l'architecture de l'app.

Destinée aux fichiers de `results/ead/ead_cor/bnr2mnesys/` (sortie de
`ead_bnr2mnesys.py`), où le `<odd>` de chaque `<c>` résume ses liens et est
considéré comme la donnée maître : quand il est corrigé à la main (ajout,
modification ou suppression d'un lien), cette app répercute la correction sur
les `<dao>`/`<daoloc>` correspondants.

## Structure

```
├── app.py              # Interface graphique (Tkinter)
├── ead_preprocess.py   # Classe EAD_preprocess (opérations de préparation)
├── img/                # Ressources graphiques (logo)
├── build/               # Spec PyInstaller + mode d'emploi Windows
├── requirements.txt
└── README.md
```

`ead_preprocess.py` importe `dao_ark` (fusion des `daogrp`, insertion des liens ARK),
un module partagé avec `scripts/ead/ead_bnr2mnesys.py` situé dans
`scripts/ead/dao_ark.py` à la racine du dépôt. L'application doit donc rester dans
un dépôt complet (pas de copie isolée du seul dossier `ead_prepublication/`).

## Lancement

```bash
python app.py
```

## Dépendances

- **Runtime** : Tkinter (bibliothèque standard) + `lxml` (voir `requirements.txt`, déjà fournie par l'environnement conda `rbx-bnr-data` du dépôt)
- **Packaging** : `pyinstaller` (voir `requirements.txt`) — uniquement pour générer un exécutable autonome

## Opérations de préparation

La logique métier se trouve dans `ead_preprocess.py`, classe `EAD_preprocess`. Une
seule opération y est implémentée à ce jour :

- `sync_dao_from_odd()` — pour chaque `<c>` possédant un `<odd>`, apparie ses `<p>`
  reconnus (format `role href [audience]`, rôle EAD parmi `ODD_ROLES`, grammaire
  documentée dans
  [Les liens DAO : structures et cas de figure](../../documentation/files/donnees/dao_daogrp.md#le-résumé-odd-donnée-maître))
  aux `<dao>`/`<daoloc>` existants **par le triplet `(href, role, audience)`**
  (pas par le `href` seul, qui peut légitimement se répéter dans un même
  `<daogrp>` sous des `role` différents — ex. un même pdf en
  `preservation:pdf`/`access:pdf`) :
  - triplet déjà présent → rien à faire, le lien est inchangé ;
  - triplet absent des `<dao>`/`<daoloc>` existants → nouveau `<daoloc>`/`<dao>`
    créé : directement dans le `<daogrp>` s'il existe déjà (**pas** via
    `dao_ark.add_ark_links`, dont la dédup par `role` seul rejetterait à tort un
    `role` déjà présent sous un autre `href` — ex. une 2ᵉ piste `access:audio`
    d'un même fonds sonore) ; sinon délègue à `add_ark_links` (`<dao>` isolé
    converti en `<daogrp>`, ou nouveau `<dao>`/`<daogrp>`) ;
  - `<dao>`/`<daoloc>` existant dont le triplet a disparu du `<odd>` → supprimé.
    Un simple changement de `role`/`audience` sur un `href` se traduit donc par
    une suppression de l'ancien triplet et un ajout du nouveau (pas de
    modification en place).

  Les `<p>` qui ne commencent par aucun rôle reconnu sont ignorés (notes
  éditoriales éventuelles du `<odd>`, sans rapport avec les dao). Le `<odd>`
  lui-même n'est jamais modifié ni supprimé : il reste la référence pour les
  exécutions suivantes. Retourne `{"ajoutes": int, "supprimes": int}`.

`transform(progress_callback)` appelle cette méthode et sérialise le résultat ;
c'est ce qu'utilise l'interface graphique.

## Exécutable Windows (sans installer Python)

Voir **`build/BUILD_WINDOWS.md`**. En résumé, sur une machine Windows :

```bat
cd app/ead_prepublication
pip install -r requirements.txt pyinstaller
pyinstaller build\ead_prepublication.spec --distpath ..\dist
```

→ `app\dist\PrepublicationEAD.exe` (autonome). Un workflow GitHub Actions
(`.github/workflows/build-windows-ead-prepublication.yml`) reconstruit aussi
automatiquement le `.exe` à chaque modification de `app/ead_prepublication/`,
disponible à deux endroits :

- **Lien stable** (toujours le dernier build réussi) :
  https://github.com/ragbx/rbx-bnr-data/releases/download/build-ead-prepublication/PrepublicationEAD.exe
- Artefact du run CI (onglet *Actions*, section *Artifacts* du run) — rétention limitée.

## Fonctionnalités UI

- Sélection du fichier source via explorateur de fichiers
- Suggestion automatique du nom de fichier de sortie
- Barre de progression animée
- Journal des opérations en temps réel
- Traitement en thread secondaire (UI non bloquée)
