# Presse ancienne — contrôles après conversion

Cette page décrit la méthode de contrôle des lots de presse ancienne une fois
convertis et déposés sur le stockage S3 par `scripts/img/img3/presse_mets.py`.

Pour chaque fascicule, la chaîne produit un JPEG de diffusion par page à partir du
TIFF de conservation, puis un fichier METS qui décrit les cinq fichiers de chaque
page : TIFF, JPEG, ALTO, texte et PDF. Avec l'option `--upload`, les JPEG et le METS
sont envoyés sur S3, contrôlés, puis supprimés du disque local.

La chaîne vérifie chaque fichier au moment où elle le produit. Les contrôles décrits
ici sont d'une autre nature : ils portent sur le lot entier, après coup, et sont
faits indépendamment du script.

État au 7 octobre 2026 : méthode proposée, aucun de ces contrôles n'est encore
scripté.

---

## 1. Ce que la chaîne vérifie déjà

Ces contrôles sont faits par `presse_mets.py` pendant le traitement. Un fascicule
qui échoue à l'un d'eux n'est pas déposé.

| Objet | Contrôle | En cas d'échec |
|---|---|---|
| TIFF | présent sous la source, MD5 identique à celui du fichier de référence | fascicule en `erreur`, rien n'est écrit |
| TIFF | décodage strict des pixels (un fichier abîmé n'est pas toléré) | fascicule en `erreur` |
| JPEG | relecture complète après écriture, dimensions égales à celles du TIFF | fascicule en `erreur`, JPEG de la tentative supprimés |
| JPEG | trace de conversion et uuid présents dans les métadonnées XMP | fascicule en `erreur` |
| METS | validation contre les schémas METS, MODS et MIX (`--validate`) | fascicule `invalide`, non envoyé |
| Objet S3 | taille et MD5 de chaque JPEG et de chaque METS après dépôt | fascicule en `erreur_envoi`, fichiers locaux conservés |

Ce que la chaîne ne voit pas :

- un fascicule absent du manifeste, donc jamais traité ;
- un JPEG techniquement valide mais visuellement faux (inversé, mal coloré) ;
- l'état réel sur S3 des ALTO, textes et PDF, qu'elle décrit sans les ouvrir ;
- le comportement des fichiers dans le visualiseur.

---

## 2. Contrôles à mener après chaque lot

### 2.1 Complétude — sur tout le lot

**But** : repérer un fascicule ou une page oubliés, et tout objet en trop.

**Méthode** : rapprocher trois sources.

- le manifeste du lot : la liste des TIFF et des fascicules attendus ;
- les récapitulatifs du lot : `presse_mets_*.csv` (une ligne par fascicule) et
  `presse_jpeg_*.csv` (une ligne par JPEG et par METS déposés) ;
- la liste réelle des objets du bucket `mediatheque-patarch-communicable`.

**Résultat attendu** :

- chaque TIFF du manifeste a un JPEG à la même clé, extension `.jpg` ;
- chaque fascicule a un METS `<fascicule>_mets.xml` dans le même répertoire ;
- aucun JPEG ni METS sur S3 hors de ceux annoncés par les récapitulatifs.

### 2.2 Tri des cas signalés — sur tout le lot

**But** : ne laisser aucun fascicule sans décision.

**Méthode** : relire le récapitulatif par fascicule et classer chaque cas.

| Colonne ou statut | Signification | Décision à prendre |
|---|---|---|
| `erreur` | TIFF manquant, MD5 différent du ref, TIFF illisible | corriger la source ou le ref, puis relancer |
| `refuse` | titre sans intitulé connu, ou fascicule sans TIFF | hors périmètre, ou compléter les données |
| `invalide` | METS non conforme aux schémas | anomalie du script, à corriger avant relance |
| `erreur_envoi` | échec du dépôt ou du contrôle S3 | relancer la même commande |
| `pages_incompletes`, `manques` | une page n'a pas ses cinq fichiers au ref | accepter, ou rechercher le fichier manquant |
| `couleur_sans_profil` | TIFF couleur sans profil ICC : couleurs reprises telles quelles | contrôle visuel de ces pages |
| METS sans ARK | fascicule absent de l'instrument de recherche | signaler pour l'EAD |

### 2.3 Qualité d'image — sur échantillon

**But** : vérifier que le JPEG restitue fidèlement le TIFF.

**Méthode** : tirer quelques pages par titre et par période, et les comparer au
TIFF d'origine.

- À l'œil : inversion noir et blanc, couleurs, orientation, page tronquée,
  lisibilité des petits caractères.
- Par mesure : un indice de ressemblance entre TIFF et JPEG, et la recherche des
  pages dont le poids est anormal pour leur surface, signe d'une page blanche ou
  d'une image corrompue.

Les pages signalées en `couleur_sans_profil` entrent d'office dans l'échantillon.

### 2.4 Cohérence du METS avec le bucket — sur échantillon

**But** : vérifier que le METS décrit bien ce qui est réellement sur S3.

**Méthode** : relire des METS depuis S3 et, pour chaque fichier de leur section
`fileSec`, vérifier que l'objet existe à la clé indiquée avec la taille et le MD5
annoncés.

Ce contrôle est le seul qui porte sur les ALTO, textes et PDF.

### 2.5 Étiquettes S3 — sur échantillon

**But** : vérifier que chaque JPEG et chaque METS porte ses étiquettes `uuid` et
`checksum_md5`.

**Limite** : l'utilisateur de lecture (`user_ro`) n'a pas le droit de lire les
étiquettes. Ce contrôle demande l'utilisateur d'écriture ou un droit de lecture
supplémentaire. Il n'a jamais été fait.

### 2.6 Usage réel — sur quelques fascicules

**But** : vérifier ce que verra le lecteur.

**Méthode** : ouvrir quelques fascicules dans le visualiseur et vérifier :

- l'affichage du JPEG ;
- le calage du texte ALTO sur l'image, surlignage d'un mot recherché compris.

**Point ouvert** : les coordonnées des ALTO ne sont pas toujours à l'échelle du
TIFF. Il reste à établir si le visualiseur les remet à l'échelle.

---

## 3. Contrôle à l'inscription au fichier de référence

Les JPEG et les METS déposés ne sont pas encore inscrits au fichier de référence.
Au moment de cette inscription, vérifier :

- l'unicité des uuid, y compris par rapport aux uuid déjà présents ;
- l'absence de collision de clés S3 ;
- les décomptes par titre : autant de JPEG que de TIFF, un METS par fascicule.

Les récapitulatifs `presse_jpeg_*.csv` sont la seule trace locale des uuid et MD5
des fichiers déposés : ils doivent être conservés jusqu'à cette inscription.

---

## 4. Étendue et ordre des contrôles

| Contrôle | Étendue | Automatisable | Accès requis |
|---|---|---|---|
| 2.1 Complétude | tout le lot | oui | lecture S3 |
| 2.2 Tri des cas signalés | tout le lot | en partie (le classement oui, la décision non) | aucun |
| 2.3 Qualité d'image | échantillon | en partie (mesures oui, examen visuel non) | TIFF et lecture S3 |
| 2.4 Cohérence METS et bucket | échantillon | oui | lecture S3 |
| 2.5 Étiquettes S3 | échantillon | oui | lecture des étiquettes |
| 2.6 Usage réel | quelques fascicules | non | visualiseur |

Ordre conseillé : 2.1 et 2.2 d'abord, parce qu'ils sont rapides, couvrent tout le
lot et conditionnent les relances ; puis 2.3 à 2.6 sur échantillon.

---

## 5. Lots déjà déposés

| Date | Manifeste | Fascicules | JPEG | METS | Contrôles faits |
|---|---|---|---|---|---|
| 2026-10-04 | `results/presse/manifeste_50_fascicules.csv` | 50 | 241 | 50 | présence sur S3 de tous les JPEG et METS ; forme d'un METS relu |
