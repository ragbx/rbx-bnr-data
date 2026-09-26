# Script : geogname_concordance.py

**Emplacement :** `scripts/ead/geogname_concordance.py`

Tient à jour une **table de concordance des noms de voies de Roubaix**
(ancien nom → voie actuelle) et repère les formes du référentiel des
`<geogname>` dont le rattachement à une voie actuelle homonyme est
**potentiellement anachronique** : un document ancien qui mentionne la
« rue Neuve » ne désigne pas forcément la rue Neuve d'aujourd'hui.

---

## Sources

Copies locales dans `data/geo/`, téléchargées au premier lancement ou avec
`--maj` :

| Fichier | Source | Contenu |
|---|---|---|
| `topo_roubaix.csv` | Fichier TOPO de la DGFiP (successeur de FANTOIR), API de data.economie.gouv.fr, filtre `code_dep='59' and code_commune='512'` — Licence Ouverte | 1 624 voies et lieux-dits de Roubaix : code voie, nature, libellé, date de création de l'article. **Pas de géométrie, pas de voies annulées** |
| `osm_roubaix_noms.json` | OpenStreetMap, API Overpass (voies nommées et relations `street`/`associatedStreet` de Roubaix, attributs seulement) — © contributeurs OpenStreetMap, licence ODbL | `name`, `old_name`, `was:name`, `alt_name`… |

Le **code voie TOPO** donne l'identifiant `59512_<code>`, commun à la BAN et
à la BD TOPO (ex. `59512_1600` = rue Darbo) : c'est l'identifiant des voies
retenu pour le référentiel, y compris pour les voies sans géométrie
actuelle. TOPO ne remonte pas avant son chargement initial : toutes les
voies existantes en 1987 y ont la date de création `19870101`.

## Entrées

| Fichier | Rôle |
|---|---|
| `data/geo/referentiel_geogname.csv` | Référentiel des formes BnR (une ligne par libellé + `source` d'origine) |
| `results/ead/indexation/rbx-bnr_geogname.csv` | Rattache chaque forme à ses composants (`eadid`, `c_id`) |
| `results/ead/ead_cor/bnr2mnesys/*.xml` | Années des documents (`unitdate`, `@normal` sinon texte) |

## Sorties

**`data/geo/concordance_renommages.csv`** — table **tenue à la main**. Le
script n'y ajoute que les candidats absents (clé : ancien nom + voie
actuelle, normalisés), avec `statut` = `à vérifier` ; il ne modifie jamais
une ligne existante : on peut corriger, valider ou compléter les lignes
(y compris depuis d'autres sources) sans risque de les perdre.

| Colonne | Contenu |
|---|---|
| `ancien_nom` | Nom ancien ou variante |
| `voie_actuelle`, `voie_actuelle_id` | Voie actuelle et son identifiant `59512_<code>` (TOPO, si trouvé) |
| `type` | `renommage` (OSM `old_name`, `was:name`) ou `variante` (OSM `alt_name`) |
| `date_creation_topo` | Date de création de la voie actuelle dans TOPO |
| `source`, `statut`, `remarque` | Provenance, validation, commentaires |

**`results/ead/indexation/rbx-bnr_appariements_a_verifier.csv`** — une ligne
par forme du référentiel à vérifier, avec la voie actuelle homonyme, le
motif, les années min/max des documents indexés et le nombre de documents
sans année. Deux motifs :

- **« ancien nom de : … »** — la forme correspond à l'ancien nom d'une voie
  de la concordance (type `renommage`) ;
- **« voie actuelle créée en … »** — la voie actuelle homonyme a été créée
  dans TOPO après 1987, alors que des documents sont plus anciens (ou sans
  date).

Ce sont des **signalements**, pas des erreurs certaines : l'index de la BnR
semble parfois employer le nom actuel pour des documents anciens (ex.
`Europe, rond point de l'` sur des documents de 1910, rond-point créé en
1988). À trancher au cas par cas, en connaissant la convention d'indexation.

## Premier passage (2026-09-26)

- Concordance : 4 renommages et 21 variantes issus d'OSM — Boulevard de
  Paris → Boulevard du Général de Gaulle, Rue Neuve → Rue du Maréchal Foch,
  Rue de l'Alma → Rue Tela Tchaï (2023), Rue Émile Moreau → Rue de la
  Redoute (2024).
- 25 formes à vérifier, dont `Alma, rue de l'` (88 occ.), `Paris,
  boulevard de` (52), `Neuve, rue` (43 ; la rue Neuve actuelle date de 2009,
  les documents de 1853 à 1950), `Emile Moreau, rue` (19), `Redoute, rue de
  la` (11 ; documents de 1884 à 1929, voie créée en 2024).

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/geogname_concordance.py          # sources locales
    python scripts/ead/geogname_concordance.py --maj    # retélécharge TOPO et OSM

L'API Overpass est parfois saturée (erreur 504) : le script réessaie et bascule
sur un serveur miroir.
