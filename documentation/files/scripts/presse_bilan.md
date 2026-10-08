# Script : presse_bilan.py

**Emplacement :** `scripts/img/img3/presse_bilan.py`

Bilan **a posteriori** de la [chaîne presse](presse_mets.md) (TIFF → JPEG de
diffusion et METS par fascicule, puis envoi sur S3). Il répond
à la question : **où en est chaque fascicule de l'extrait ?** Envoyé, en erreur (et
pourquoi), ou jamais traité.

Il est utile après un ou plusieurs lancements de `presse_mets.py --upload` (ou de
`presse_upload.py --execute`), car l'information est alors dispersée :

- chaque lancement écrit **son propre** récapitulatif horodaté ; après des relances,
  l'état d'un fascicule est celui de sa dernière ligne, tous fichiers confondus ;
- un fascicule **abandonné** (lot arrêté après `--max-echecs-envoi` échecs, ou
  interrompu brutalement) n'a **aucune ligne** dans les récapitulatifs ;
- seul S3 dit ce qui y est réellement déposé.

Le script ne modifie rien : il lit des CSV et, avec `--s3`, interroge S3 en lecture
seule.

---

## Entrées

| Entrée | Obligatoire | Contenu |
|---|---|---|
| `extrait` | oui | L'extrait du fichier de référence passé à `presse_mets.py` (`.csv` ou `.csv.gz`), par exemple `results/presse/manifeste_1000_fascicules.csv`. Il donne la liste des fascicules attendus, lue exactement comme le fait `presse_mets.py` (mêmes filtres, même découpage en fascicules). |
| `recaps` | oui (un ou plusieurs) | Les récapitulatifs par fascicule `presse_mets_*.csv` de **tous** les lancements. On peut passer des fichiers, ou des dossiers (les `--out-dir`) dans lesquels ils sont cherchés. |
| `--uploads` | non | Les résultats de `presse_upload.py --execute` (`*_upload_*.csv`), si l'envoi a été fait à part. Les simulations sont ignorées. |
| `--s3` | non | Vérifie sur S3 la présence du METS de chaque fascicule. |

Les récapitulatifs par fichier (`presse_jpeg_*.csv`) ne sont **pas** lus : ils ne
portent que les fascicules au statut `ok`.

Les récapitulatifs sont lus du plus ancien au plus récent, d'après l'horodatage
`AAAAMMJJHHMMSS` de leur nom (à défaut, la date du fichier).

---

## Utilisation

    # d'après les seuls récapitulatifs
    python scripts/img/img3/presse_bilan.py extrait_ref.csv sortie/

    # avec vérification sur S3 (recommandé)
    python scripts/img/img3/presse_bilan.py extrait_ref.csv sortie/ --s3

    # envoi fait à part avec presse_upload.py
    python scripts/img/img3/presse_bilan.py extrait_ref.csv sortie/ --uploads "sortie/*_upload_*.csv" --s3

| Option | Rôle |
|---|---|
| `--uploads` | Résultats de `presse_upload.py` (fichiers ou motifs). |
| `--s3` | Interroge S3 (un `head_object` par METS). |
| `--bucket` | Bucket interrogé (défaut : `mediatheque-patarch-communicable`). |
| `--workers` | Appels simultanés à S3 (défaut : 16). |
| `--sets` | Sets OAI donnant les titres des journaux (défaut : celui de `presse_mets.py`). |
| `--csv-out` | Chemin du bilan (défaut : `presse_bilan_AAAAMMJJHHMMSS.csv` dans le dossier du dernier récapitulatif). |

L'accès S3 est celui de `scripts/s3/rbx_s3.py` (`conf.yml`), avec l'utilisateur en
lecture seule `user_ro`.

---

## États

| État | Signification | Suite à donner |
|---|---|---|
| `envoye` | Avec `--s3` : le METS est sur S3. Sans `--s3` : envoi réussi d'après les récapitulatifs. | — |
| `a_verifier` | Envoyé d'après les récapitulatifs, mais METS absent de S3. | Relancer le fascicule ; comprendre l'écart. |
| `erreur_envoi` | L'envoi a échoué ; `cause` donne la clé fautive et le message. | Relancer `presse_mets.py --upload` : les fichiers sont restés sous `--out-dir`. |
| `erreur` | Contrôle, conversion ou METS en échec (TIFF absent, MD5 différent du ref, TIFF abîmé…). | Voir `cause`. |
| `invalide` | METS refusé à la validation XSD. | Voir `cause`. |
| `refuse` | Écarté d'emblée : aucun TIFF dans l'extrait, ou titre inconnu (pas de set OAI). | Voir `cause`. |
| `converti_non_envoye` | JPEG et METS produits, sans envoi connu. | Envoyer (`presse_upload.py` ou `--upload`). |
| `jamais_traite` | Aucune ligne dans aucun récapitulatif. | Relancer le lot. |

**S3 fait foi.** Le METS d'un fascicule part en dernier, après ses JPEG : sa présence
sur S3 vaut fascicule complet. Avec `--s3`, un fascicule dont le METS est présent est
donc `envoye` quel que soit son dernier statut aux récapitulatifs.

**Sans `--s3`**, un fascicule `ok` est tenu pour envoyé si son `t_envoi_s` est
supérieur à 0 : le récapitulatif ne dit pas si `--upload` était posé. L'état repose
alors sur les seuls récapitulatifs, ce que le script rappelle à l'écran.

---

## Sortie

Un CSV, une ligne par fascicule de l'extrait, trié par fascicule :

| Colonne | Contenu |
|---|---|
| `fascicule`, `corpus_code`, `pages` | Identité du fascicule et nombre de pages à l'extrait. |
| `etat` | L'un des états ci-dessus. |
| `mets_s3` | `oui` / `non` avec `--s3`, vide sinon. |
| `dernier_status` | Statut de sa dernière ligne aux récapitulatifs (`ok`, `erreur_envoi`, `deja_envoye`…). |
| `dernier_recap` | Récapitulatif qui porte cette ligne. |
| `passages` | Nombre de récapitulatifs où le fascicule apparaît. |
| `cause` | Message d'erreur ramené sur une ligne, sans la trace Python (ex. « unable to call jpegsave ; » suivi du motif donné par libvips). |

À l'écran : le décompte par état, en fascicules et en pages. Les fascicules présents
dans les récapitulatifs mais absents de l'extrait sont ignorés et signalés.

Le code de sortie vaut **2** dès qu'un fascicule n'est pas `envoye`.

---

## Limites

- Un récapitulatif écrit sous un autre nom (`--csv-out` de `presse_mets.py`) n'est pas
  trouvé dans un dossier : le passer comme fichier.
- Pour un fascicule en `erreur_envoi`, seule la première clé en échec est connue : ses
  JPEG déjà partis ne sont consignés nulle part. Une relance les retrouve sur S3
  (`deja_present`) sans les renvoyer.
- Le contrôle S3 porte sur la **présence** du METS, pas sur le contenu des objets :
  taille et MD5 sont contrôlés à l'envoi, par `presse_upload.py`.

---

## Voir aussi

- [Chaîne presse](presse_mets.md) — la chaîne dont ce script fait le bilan.
- [Scripts S3](s3.md) — accès au stockage.
