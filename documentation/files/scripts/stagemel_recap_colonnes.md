# Récapitulatif d'audit par corpus : colonnes CAS et STATUT

*État au 24/09/2026* — voir aussi [Chaîne `stagemel_*`](stagemel.md), qui produit ces fichiers

## Les deux sources comparées

Le récapitulatif compare, pour un corpus, deux listes de fichiers qui devraient correspondre :

- **Le REF** (fichier de référence) : l'inventaire de tous les fichiers numérisés réellement présents sur les espaces de stockage (serveur Azraël, disques externes, stockage S3). Pour chaque fichier, il donne son nom, son emplacement, son identifiant (UUID) et son **statut de conservation** : versé sur S3, à supprimer, non décidé… Voir [Fichier de référence](../donnees/fichier_ref.md).
- **Les notices EAD** : les instruments de recherche publiés sur la bn-r. Chaque notice cite par leur nom les fichiers numérisés qui illustrent le document décrit (balises `<dao>`, d'où le terme « DAO » ci-dessous).

La comparaison se fait sur le nom du fichier, sans son extension.

## Lire un récapitulatif

Le récapitulatif compte une ligne par fichier trouvé dans l'une ou l'autre source. Trois colonnes portent l'essentiel : **CAS** indique dans quelle source le fichier a été trouvé, **STATUT** où il en est, **À FAIRE** l'action à mener quand elle est déjà décidée.

Fichier : `results/corpus/stagemel/recap/<corpus>_recap_<date>.xlsx` (38 corpus au 24/09/2026).

| Colonne | Contenu |
| --- | --- |
| `CAS` | APPARIÉ, REF SEUL ou DAO SEUL (section suivante) |
| `<corpus>` | Nom du fichier : celui du REF, ou celui cité par la notice pour un DAO SEUL |
| `UUID` | Identifiant du fichier dans le REF ; vide pour un DAO SEUL, sauf erreur de nom probable (ERREUR DAO) |
| `STATUT` | Statut de conservation repris du REF, ou mention propre aux fichiers absents du REF |
| `CHEMIN` | Emplacement du fichier sur le stockage ; vide pour un DAO SEUL |
| `PROBLEMES` | Anomalies repérées automatiquement |
| `À FAIRE` | Action à mener ; **vide quand la décision reste à prendre** |

Le second onglet, `pivot`, compte les lignes par CAS et par STATUT : c'est la vue d'ensemble à lire en premier.

Le récapitulatif **constate**, il ne tranche pas : il ne modifie aucun fichier et ne décide rien sur les cas ambigus.

## Colonne CAS

| CAS | Dans le REF | Cité par une notice | Ce que ça signale | Lignes au 24/09 |
| --- | --- | --- | --- | --- |
| **APPARIÉ** | oui | oui | Situation normale : le fichier existe et une notice le cite | 110 326 |
| **REF SEUL** | oui | non | Fichier numérisé qu'aucune notice ne cite : notice manquante, fichier hors du périmètre, ou nom différent de celui de la notice | 59 257 |
| **DAO SEUL** | non | oui | Notice qui cite un fichier introuvable : document pas encore numérisé, ou nom mal écrit d'un côté ou de l'autre | 10 890 |

Deux précisions :

- Un fichier du REF appartient au corpus par son code corpus. Un nom cité par une notice lui est rattaché s'il commence par le code du corpus, éventuellement précédé de `RBX_` (ou d'une variante mal saisie comme `RBx_` ou `BX_`).
- Un même nom peut correspondre à plusieurs fichiers du REF, par exemple le `.jpg` et le `.tif` d'une même page. Chacun a sa propre ligne, et tous sont APPARIÉS si la notice cite ce nom.

10 corpus sont entièrement en REF SEUL, car aucune notice ne les décrit : MED_AVI, MED_DIL, MED_FAN, MED_NPT, MED_PBI, MED_PER, MED_VAI, MED_VID, AMR_OBJ, et MED_JOU (6 fichiers cités seulement).

## Colonne STATUT

Pour APPARIÉ et REF SEUL, STATUT reprend tel quel le statut de conservation du REF. Pour DAO SEUL, le fichier n'existe pas dans le REF : la colonne porte une mention propre.

### Statuts repris du REF (APPARIÉ, REF SEUL)

Ces statuts sont des décisions déjà prises. Pour un fichier APPARIÉ, À FAIRE les traduit directement en action. Définitions reprises du [Fichier de référence](../donnees/fichier_ref.md).

| Famille | STATUT | Sens | À FAIRE (APPARIÉ) |
| --- | --- | --- | --- |
| Fait | `TRANSFERT_S3_OK` | Déjà versé sur le stockage S3 | Rien à faire |
| À verser | `À TRANSFERER` | À verser sur S3 | À transférer |
| À verser | `À TRANSFERER APRES VALIDATION` | Versement décidé, pas encore fait | À transférer (après validation) |
| À verser | `À TRANSFERER VOIR MARIE` | À verser, sous réserve de l'avis de Marie | À transférer (voir Marie) |
| À verser | `EN LIGNE - À TRANSFERER ?` | Consultable en ligne, versement sur S3 à confirmer | À transférer (à confirmer) |
| À supprimer | `À SUPPRIMER (DIFFUSION)` | Copie de diffusion, sans valeur de conservation | À supprimer |
| À supprimer | `À SUPPRIMER (DOUBLON)` | Doublon exact (contenu identique vérifié) | À supprimer |
| À supprimer | `À SUPPRIMER (DOUBLON AZ)` | Doublon sur le serveur Azraël | À supprimer |
| À supprimer | `À SUPPRIMER (DOUBLON S3-AZ)` | Doublon entre S3 et Azraël | À supprimer |
| À supprimer | `À SUPPRIMER (REMPLACEMENT)` | Remplacé par une version vérifiée | À supprimer |
| À supprimer | `À SUPPRIMER (RENOMMAGE)` | Ancienne version d'un fichier renommé | À supprimer |
| À supprimer | `À SUPPRIMER (FILE_TYPE)` | Type de fichier qu'on ne conserve pas | À supprimer |
| À supprimer | `À SUPPRIMER (TESTS)` | Fichier de test | À supprimer |
| À supprimer | `À SUPPRIMER (MED_PAR)` | Suppression propre au corpus MED_PAR | À supprimer |
| À supprimer | `À SUPPRIMER (DIFFUSION - MASTER DÉJÀ SUR S3)` | Copie de diffusion dont l'original est déjà sur S3 | À supprimer |
| À supprimer | `À SUPPRIMER (DOUBLON SOURCE - À VERIFIER)` | Doublon probable, à contrôler | À supprimer (vérifier un échantillon avant) |
| À examiner | `DOUBLON - À VOIR` | Doublon soupçonné | À examiner (doublon) |
| À examiner | `À CHERCHER` | Fichier à localiser | À chercher |
| À examiner | `S3_KEY À CONSTRUIRE` | Emplacement sur S3 encore à définir | S3 key à construire |
| Non décidé | `INCONNU` | Aucun statut attribué, reste à trier | **vide** |
| Non décidé | `INCONNU FRAD59` | Fonds versé par les Archives départementales du Nord, à trancher avec elles | **vide** |
| Non décidé | `INCONNU VOIR MARIE` | À trancher avec Marie | **vide** |

Les statuts `INCONNU` n'ont volontairement aucune action : la règle à appliquer à chaque corpus reste à décider.

### Mentions propres aux DAO SEUL

| STATUT | Sens | À FAIRE |
| --- | --- | --- |
| `SEUL DAO` | Aucun fichier du REF ne porte ce nom, même en ignorant majuscules et ponctuation | À numériser |
| `ERREUR DAO (candidat)` | Un fichier du REF porte le même nom, aux majuscules ou à la ponctuation près : faute de frappe probable, dans la notice ou dans le REF | **vide**, à confirmer en relisant la notice |
| `ERREUR DAO (candidat, fichier À SUPPRIMER)` | Idem, mais le seul fichier ressemblant est déjà marqué À SUPPRIMER : à vérifier en priorité | **vide** |

Pour une ERREUR DAO, la colonne UUID donne l'identifiant du fichier du REF qui ressemble. La recherche privilégie les fichiers qui ne sont pas marqués À SUPPRIMER.

## Combinaisons CAS × STATUT

En résumé : **À FAIRE n'est rempli que pour un fichier APPARIÉ dont le statut est décidé, ou pour un SEUL DAO**. Toutes les autres combinaisons attendent une décision.

| CAS | STATUT | Lecture | À FAIRE | Lignes au 24/09 |
| --- | --- | --- | --- | --- |
| APPARIÉ | `TRANSFERT_S3_OK` | Cas normal : fichier cité et versé | Rien à faire | 105 444 |
| APPARIÉ | `À TRANSFERER…`, `EN LIGNE - À TRANSFERER ?` | Fichier cité, versement décidé mais pas fait | À transférer (+ réserve éventuelle) | 4 560 |
| APPARIÉ | `À SUPPRIMER (…)` | Fichier cité mais voué à disparaître, sans original déjà sur S3 : vérifier que la notice renverra encore vers un fichier conservé | À supprimer | 256 |
| APPARIÉ | `DOUBLON - À VOIR` | Doublon soupçonné | À examiner | 34 |
| APPARIÉ | `INCONNU…` | Fichier cité, statut non décidé | vide | 32 |
| REF SEUL | `TRANSFERT_S3_OK` | Fichier versé que rien ne cite : notice à créer ou nom à corriger | vide | 35 115 |
| REF SEUL | `INCONNU…` | Ni notice ni statut : le plus gros volume à trier | vide | 22 874 |
| REF SEUL | `À SUPPRIMER (…)` | Fichier non cité et déjà voué à disparaître, sans original sur S3 : vérifier qu'aucune version n'est perdue | vide | 1 193 |
| REF SEUL | `À TRANSFERER`, `EN LIGNE - À TRANSFERER ?`, `DOUBLON - À VOIR` | Statut décidé, mais aucune notice | vide | 75 |
| DAO SEUL | `SEUL DAO` | Notice sans fichier : document à numériser, ou nom mal écrit non repéré | À numériser | 10 788 |
| DAO SEUL | `ERREUR DAO (…)` | Nom cité qui diffère d'un fichier du REF par les majuscules ou la ponctuation | vide | 102 |

À FAIRE reste vide pour **tous les REF SEUL**, même déjà versés : le statut dit quoi faire du fichier sur le stockage, mais pas s'il faut créer une notice, corriger un nom ou écarter le fichier.

Un REF SEUL et un DAO SEUL du même corpus peuvent désigner le même document sous deux noms trop différents pour être rapprochés automatiquement. À vérifier avant de lancer une numérisation.

## Colonne PROBLEMES et lignes écartées

PROBLEMES signale des anomalies repérées automatiquement. Elle ne change jamais le contenu d'À FAIRE.

| CAS | PROBLEMES | Sens | Lignes au 24/09 |
| --- | --- | --- | --- |
| APPARIÉ | *(vide)* | Rien à signaler | 104 738 |
| APPARIÉ | Pas de format tif | Image `.jpg` sans `.tif` du même nom dans le corpus : aucun fichier de conservation connu | 5 588 |
| REF SEUL | Absent des notices EAD | Toujours présent pour ce CAS | 43 297 |
| REF SEUL | Absent des notices EAD ; Pas de format tif | Idem, et aucun `.tif` | 15 960 |
| DAO SEUL | Fichier non retrouvé dans REF | Accompagne `SEUL DAO` | 10 788 |
| DAO SEUL | Clé proche d'un fichier connu (casse/ponctuation ?) | Accompagne `ERREUR DAO (…)` | 102 |

Le contrôle « Pas de format tif » ignore les majuscules (`.JPG` = `.jpg`) et accepte les variantes `.jpeg` et `.tiff`.

**Lignes écartées du récapitulatif** : un fichier marqué `À SUPPRIMER (…)` n'apparaît pas si son original `.tif` est déjà versé sur S3 (`TRANSFERT_S3_OK`). Cet original est cherché dans tout le REF, quel que soit son corpus. La suppression ne fait alors rien perdre et il n'y a rien à décider. Au 24/09, 28 662 lignes sont écartées ainsi (26 405 APPARIÉ, 2 257 REF SEUL).

## Points de vigilance

- **ERREUR DAO est une piste, pas une certitude.** Seuls les écarts de majuscules et de ponctuation sont repérés ; il faut relire la notice avant de corriger un nom.
- **Les fautes de frappe plus lourdes ne sont pas repérées.** Un nom avec un chiffre en trop ou un folio décalé sort à la fois en DAO SEUL et en REF SEUL. Une recherche par ressemblance a été essayée puis abandonnée : sur les manuscrits (MED_MS), les folios voisins se ressemblent tous.
- **« À numériser » est l'explication la plus fréquente d'un SEUL DAO, pas la seule** : vérifier d'abord qu'aucun REF SEUL du corpus ne correspond.
- **Un corpus sans aucune notice** sort entièrement en REF SEUL : le problème concerne le corpus entier, pas chaque fichier.
- **Dossier « Zébulon »** : les fichiers qui ne sont ni dans le REF ni cités par une notice ne peuvent pas apparaître, faute de données sur ce dossier.
- **Anciens récapitulatifs** : ceux du stage classaient en APPARIÉ tous les fichiers du REF, même non cités, et comptaient en double les fichiers cités par plusieurs notices. Les récapitulatifs antérieurs au 24/09/2026 sont à écarter.
