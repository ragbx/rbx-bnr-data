from datetime import datetime
from os.path import join

import pandas as pd

today = datetime.now().strftime("%Y%m%d")

date_ref = "20251226"
ref = pd.read_csv(join("results", "ref", f"_ref_files_{date_ref}.csv.gz"))

v_files = ref[ref["corpus_code"] == "VAH_PUB"]
v_files.to_csv(join("results", "corpus", f"vah_pub_files_{today}.csv.gz"), index=False)

dao = pd.read_csv(join("results", "dao", "liste_dao_flat_20260430.csv.gz"))
dao = dao[~dao["nom_fichier_base"].isna()]
v_dao = dao[dao["nom_fichier_base"].str.contains("VAH_PUB")]
v_dao.to_csv(join("results", "corpus", f"vah_pub_dao_{today}.csv.gz"), index=False)
