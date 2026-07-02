# Glossaire

**ARK** — Identifiant pérenne (Archival Resource Key). Les anciens ARK de la bn-r
(`https://www.bn-r.fr/ark:/20179/<osiros_id>`) sont reportés dans les fichiers EAD
transformés pour Mnesys afin de garder la trace des anciennes URL.

**Azraël** — Nom du serveur de stockage actuel des fichiers numérisés. Source
principale des inventaires de fichiers (`scripts/azrael/`).

**Bucket** — Espace de stockage sur le serveur S3. Le projet en utilise deux :
`mediatheque-patarch-communicable` et `mediatheque-patarch-incommunicable`,
selon la communicabilité des documents.

**checksum_md5** — Empreinte MD5 calculée pour chaque fichier numérisé et
enregistrée dans le fichier de référence. Reportée en tag sur les objets S3
au moment du versement, elle permet de vérifier l'intégrité des fichiers
d'un support à l'autre.

**Corpus** — Ensemble documentaire cohérent ayant fait l'objet d'une campagne de
numérisation, identifié par un `corpus_code` (ex. `AMR_2I`, `VAH_PUB`).
Voir [Données : corpus](donnees/corpus.md).

**Cote** — Identifiant d'une unité documentaire dans un fonds d'archives.
Encodée dans la balise `<unitid>` des fichiers EAD.

**Dao** — Digital Archival Object : balise EAD (`<dao>`, `<daogrp>`, `<daoloc>`)
qui lie une description archivistique à un fichier numérique. Les rôles utilisés
dans le projet distinguent `access:` (consultation), `preservation:`
(conservation S3) et `publication:previous` (ancien ARK bn-r).

**EAD** — Encoded Archival Description : format XML de description archivistique,
utilisé pour encoder les instruments de recherche de la bn-r.

**Fichier de conservation** — Fichier maître issu de la numérisation (TIFF haute
définition principalement), destiné à la conservation pérenne sur S3. S'oppose
au **fichier de diffusion**, version allégée destinée à la consultation en ligne.

**Fichier de référence** — Fichier central du projet
(`results/ref/_ref_files_{date}.csv.gz`) recensant l'ensemble des fichiers
numérisés avec leurs métadonnées et leur statut dans les différents chantiers.
Voir [Fichier de référence](donnees/fichier_ref.md).

**IR (instrument de recherche)** — Document décrivant un fonds d'archives et
permettant d'y rechercher. Les IR de la bn-r sont encodés en EAD.

**MIX** — Schéma de métadonnées techniques pour les images numériques
(NISO Metadata for Images in XML). Alimente les colonnes `mix_*` du fichier
de référence.

**OAI-PMH** — Protocole de moissonnage de métadonnées. Le serveur OAI de la bn-r
expose les notices des documents numérisés ; son moissonnage permet de relier
les `osiros_id` aux cotes des IR. Voir [Fichier de référence](donnees/fichier_ref.md).

**osiros_id** — Identifiant d'une notice dans l'outil bn-r. Sert à construire
les anciens ARK.

**S3** — Stockage objet (protocole Amazon S3) utilisé pour la conservation
pérenne des fichiers numérisés. Voir [Scripts S3](scripts/s3.md).

**UUID** — Identifiant unique stable attribué à chaque fichier numérisé dans le
fichier de référence. C'est la version 4 qui est utilisée dans le cadre de ce projet. Reporté en tag sur les objets S3, il fait le lien entre
les supports et le fichier de référence.
