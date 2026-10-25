#!/usr/bin/env python3
r"""lot_mets.py - med_mets.py (JPEG de diffusion et METS par notice) pour six corpus de plus.

Meme traitement que les fonds MED_MS, MED_VDM et MED_PAR (voir med_mets.py et sa
documentation), applique a MED_IMA, MED_CP, MED_AFF, MUS_VAI, MED_MON et MED_PUB. Ces
corpus ont ete verifies sans anomalie par med_mets.build_jobs : aucune notice refusee,
toutes retrouvees dans l'IR bnr2mnesys, titre dans les sets OAI. Les options sont celles de
presse_mets.py ; le manifeste est results/img/manifeste_6_corpus.csv.gz
(manifestes_lot.py), le lot complet scripts/img/img3/lot_6_corpus.sh.

Ne pas l'employer pour LAI (med_mets.unit_id suppose un « _ » dans le corpus_code), pour
AMR_PHO et AMR_AFF (pas d'IR bnr2mnesys) ni pour LAR_PUB (TIFF a JPEG old-style) : les
lignes d'un autre corpus sont ecartees sans message.
"""

import med_mets as mm

mm.CORPORA = ("MED_IMA", "MED_CP", "MED_AFF", "MUS_VAI", "MED_MON", "MED_PUB")

if __name__ == "__main__":
    mm.main()
