# Règles de nommage par corpus et impact de l'indicateur Z

Source : `results/ref/_ref_files_20260630.csv.gz` — 2 205 685 s3_key  70 corpus. Analyse en lecture seule.

Indicateur inséré juste avant le numéro de vue : `_ZA_` conservation, `_ZD_` diffusion, `_ZO_` OCR. Les fichiers METS gardent leur nom (suffixe `.mets.xml`).

Notation des motifs : `N3` = nombre à 3 chiffres, `<SERIE>` = sigle en lettres, `<L>` = lettre seule, `#` = chiffres collés à des lettres.

## AMR_10FI — 164 clés — 1 - règle directe

- **Extensions** : tif 164
- **Rôles** : ZA 164
- **Préfixe commun** : `RBX_AMR_PHO_10FI` ; **numéro de vue** : N2 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_10FI_N3_N2` — 162 (99%) — ex. `RBX_AMR_PHO_10FI_001_02.tif`
  - `RBX_<SERIE>_<SERIE>_#Fi_N3_N2` — 2 (1%) — ex. `RBX_AMR_PHO_10Fi_003_14.tif`
- **Impact de l'indicateur** : 164 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_PHO_10FI_001_02.tif` → `RBX_AMR_PHO_10FI_001_ZA_02.tif`
  - 164 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_21J — 285 clés — 1 - règle directe

- **Extensions** : tif 285
- **Rôles** : ZA 285
- **Préfixe commun** : `RBX_AMR_PHO_21J` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_21J_N3` — 285 (100%) — ex. `RBX_AMR_PHO_21J_001.tif`
- **Impact de l'indicateur** : 285 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_PHO_21J_001.tif` → `RBX_AMR_PHO_21J_ZA_001.tif`
  - 285 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_2I — 15 703 clés — 1 - règle directe

- **Extensions** : jpg 15703
- **Rôles** : ZD 15703
- **Préfixe commun** : `RBX_AMR_2I` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_2I_N3_N3` — 15 703 (100%) — ex. `RBX_AMR_2I_072_001.JPG`
- **Impact de l'indicateur** : 15 703 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_2I_072_001.JPG` → `RBX_AMR_2I_072_ZD_001.JPG`
  - 15703 image(s) de diffusion sans master de même nom
  - 15306 extension(s) en majuscules
  - 15703 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_310W — 1 136 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : tif 1136
- **Rôles** : ZA 1136
- **Préfixe commun** : `RBX_AMR_PHO_310W` ; **numéro de vue** : N2 ; **3 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_310W_N3_N2` — 1 078 (95%) — ex. `RBX_AMR_PHO_310W_001_01.tif`
  - `RBX_AMR_PHO_310W_N3_N2_N2` — 55 (5%) — ex. `RBX_AMR_PHO_310W_064_01_01.tif`
  - `RBX_AMR_PHO_310W_#-#` — 3 (0%) — ex. `RBX_AMR_PHO_310W_163-1.tif`
- **Impact de l'indicateur** : 1 078 clés (94.9%) suivent la règle directe — ex. `RBX_AMR_PHO_310W_001_01.tif` → `RBX_AMR_PHO_310W_001_ZA_01.tif`
  - 55 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_PHO_310W_064_01_01.tif`)
  - 3 nom(s) sans numéro final (intervalle 3 ; ex. `RBX_AMR_PHO_310W_163-1.tif`)
  - 1136 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_3D — 246 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : jpg 222, tif 24
- **Rôles** : ZD 222, ZA 24
- **Préfixe commun** : `RBX_AMR_3D` ; **numéro de vue** : N3/N2/N1 ; **4 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_3D_N3_N3` — 199 (81%) — ex. `RBX_AMR_3D_130_001.JPG`
  - `RBX_AMR_3D_N3_N1_N2` — 24 (10%) — ex. `RBX_AMR_3D_195_1_01.tif`
  - `RBX_AMR_3D_N3_N3_N2` — 17 (7%) — ex. `RBX_AMR_3D_135_010_01.JPG`
  - … 1 autre(s) motif(s)  6 clés
- **Impact de l'indicateur** : 223 clés (90.7%) suivent la règle directe — ex. `RBX_AMR_3D_195_1_01.tif` → `RBX_AMR_3D_195_1_ZA_01.tif`
  - 23 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_3D_135_010_01.JPG`)
  - 222 image(s) de diffusion sans master de même nom
  - 222 extension(s) en majuscules
  - 246 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_3FI — 4 843 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 4843
- **Rôles** : ZA 4843
- **Préfixe commun** : `RBX_AMR_PHO_3FI` ; **numéro de vue** : N5/N2 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_3FI_N5` — 4 822 (100%) — ex. `RBX_AMR_PHO_3FI_00001.tif`
  - `RBX_AMR_PHO_3FI_N5_N2` — 21 (0%) — ex. `RBX_AMR_PHO_3FI_03934_01.tif`
- **Impact de l'indicateur** : 4 822 clés (99.6%) suivent la règle directe — ex. `RBX_AMR_PHO_3FI_00001.tif` → `RBX_AMR_PHO_3FI_ZA_00001.tif`
  - 21 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_PHO_3FI_03934_01.tif`)
  - 4843 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_440W — 15 clés — 1 - règle directe

- **Extensions** : tif 15
- **Rôles** : ZA 15
- **Préfixe commun** : `RBX_AMR_PHO_440W` ; **numéro de vue** : N2/N3 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_440W_N3_N3_N2` — 12 (80%) — ex. `RBX_AMR_PHO_440W_038_001_01.tif`
  - `RBX_AMR_PHO_440W_N3_N3` — 3 (20%) — ex. `RBX_AMR_PHO_440W_047_009.tif`
- **Impact de l'indicateur** : 15 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_PHO_440W_038_001_01.tif` → `RBX_AMR_PHO_440W_038_001_ZA_01.tif`
  - 15 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_4FI — 326 clés — 1 - règle directe

- **Extensions** : tif 326
- **Rôles** : ZA 326
- **Préfixe commun** : `RBX_AMR_PHO_4FI` ; **numéro de vue** : N5 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_4FI_N5` — 326 (100%) — ex. `RBX_AMR_PHO_4FI_00001.tif`
- **Impact de l'indicateur** : 326 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_PHO_4FI_00001.tif` → `RBX_AMR_PHO_4FI_ZA_00001.tif`
  - 326 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_500W — 1 447 clés — 4 - règle propre au corpus nécessaire

- **Extensions** : tif 1447
- **Rôles** : ZA 1447
- **Préfixe commun** : `RBX_AMR_PHO_500W` ; **numéro de vue** : N2/N3 ; **5 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_500W_#-#` — 437 (30%) — ex. `RBX_AMR_PHO_500W_041-01.tif`
  - `RBX_AMR_PHO_500W_N3_N2` — 423 (29%) — ex. `RBX_AMR_PHO_500W_001_01.tif`
  - `RBX_AMR_PHO_500W_N3` — 386 (27%) — ex. `RBX_AMR_PHO_500W_278.tif`
  - … 2 autre(s) motif(s)  201 clés
- **Impact de l'indicateur** : 941 clés (65.0%) suivent la règle directe — ex. `RBX_AMR_PHO_500W_278.tif` → `RBX_AMR_PHO_500W_ZA_278.tif`
  - 506 nom(s) sans numéro final (intervalle 506 ; ex. `RBX_AMR_PHO_500W_041-01.tif`)
  - 1447 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_507W — 496 clés — 4 - règle propre au corpus nécessaire

- **Extensions** : tif 496
- **Rôles** : ZA 496
- **Préfixe commun** : `RBX_AMR_507W` ; **numéro de vue** : - ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_507W_N3_N3_<L>` — 496 (100%) — ex. `RBX_AMR_507W_001_002_T.tif`
- **Impact de l'indicateur** : 0 clés (0.0%) suivent la règle directe
  - 496 nom(s) sans numéro final (lettre_finale 496 ; ex. `RBX_AMR_507W_001_002_T.tif`)
  - 496 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_512W — 857 clés — 1 - règle directe

- **Extensions** : tif 857
- **Rôles** : ZA 857
- **Préfixe commun** : `RBX_AMR_PHO_512W` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_512W_N3` — 857 (100%) — ex. `RBX_AMR_PHO_512W_001.tif`
- **Impact de l'indicateur** : 857 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_PHO_512W_001.tif` → `RBX_AMR_PHO_512W_ZA_001.tif`
  - 857 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_5FI — 63 clés — 1 - règle directe

- **Extensions** : tif 63
- **Rôles** : ZA 63
- **Préfixe commun** : `RBX_AMR_PHO_5FI` ; **numéro de vue** : N5 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_5FI_N5` — 63 (100%) — ex. `RBX_AMR_PHO_5FI_00001.tif`
- **Impact de l'indicateur** : 63 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_PHO_5FI_00001.tif` → `RBX_AMR_PHO_5FI_ZA_00001.tif`
  - 63 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_5H — 7 072 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : jpg 7072
- **Rôles** : ZD 7072
- **Préfixe commun** : `RBX_AMR_VIC_5H` ; **numéro de vue** : N3/N2 ; **5 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_VIC_5H_N3_N3` — 5 830 (82%) — ex. `RBX_AMR_VIC_5H_002_001.JPG`
  - `RBX_AMR_VIC_5H_N3_N3_N2` — 909 (13%) — ex. `RBX_AMR_VIC_5H_004_045_01.JPG`
  - `RBX_<SERIE>_<SERIE>_#H#_N3_N2` — 325 (5%) — ex. `RBX_AMR_VIC_5H074_010_01.JPG`
  - … 2 autre(s) motif(s)  8 clés
- **Impact de l'indicateur** : 6 158 clés (87.1%) suivent la règle directe — ex. `RBX_AMR_VIC_5H074_010_01.JPG` → `RBX_AMR_VIC_5H074_010_ZD_01.JPG`
  - 909 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_VIC_5H_004_045_01.JPG`)
  - 5 nom(s) sans numéro final (code_sans_separateur 5 ; ex. `RBX_AMR_VIC_5H_031_056_PJ01.JPG`)
  - 7072 image(s) de diffusion sans master de même nom
  - 7072 extension(s) en majuscules
  - 7072 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_6H — 76 933 clés — 2 - règle directe + exceptions à lister

- **Extensions** : jpg 76933
- **Rôles** : ZD 76933
- **Préfixe commun** : `RBX_AMR_6H` ; **numéro de vue** : N3/N2 ; **347 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_6H_N3_N3` — 76 434 (99%) — ex. `RBX_AMR_6H_105_001.JPG`
  - `RBX_AMR_6H_N3_N3_N2` — 153 (0%) — ex. `RBX_AMR_6H_125_062_01.JPG`
  - `RBX_AMR_6H_N3_N2_N2` — 2 (0%) — ex. `RBX_AMR_6H_264_58_01.JPG`
  - … 344 autre(s) motif(s)  344 clés
- **Impact de l'indicateur** : 76 436 clés (99.4%) suivent la règle directe — ex. `RBX_AMR_6H_105_001.JPG` → `RBX_AMR_6H_105_ZD_001.JPG`
  - 154 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_6H_125_062_01.JPG`)
  - 343 nom(s) sans numéro final (copie_(n) 332, autre 11 ; ex. `6 H 165 fiche (0).JPG`)
  - 76933 image(s) de diffusion sans master de même nom
  - 76932 extension(s) en majuscules
  - 336 nom(s) avec espace, parenthèse ou point
  - 76933 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_7J — 4 clés — 4 - règle propre au corpus nécessaire

- **Extensions** : tif 4
- **Rôles** : ZA 4
- **Préfixe commun** : `RBX_AMR_PHO` ; **numéro de vue** : - ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_#J#` — 4 (100%) — ex. `RBX_AMR_PHO_7J42.tif`
- **Impact de l'indicateur** : 0 clés (0.0%) suivent la règle directe
  - 4 nom(s) sans numéro final (code_sans_separateur 4 ; ex. `RBX_AMR_PHO_7J42.tif`)
  - 4 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_8J — 98 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 98
- **Rôles** : ZA 98
- **Préfixe commun** : `RBX_AMR_PHO_8J` ; **numéro de vue** : N3/N2 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_8J_N2_N3` — 96 (98%) — ex. `RBX_AMR_PHO_8J_52_000.tif`
  - `RBX_AMR_PHO_8J_N2_N3_N2` — 2 (2%) — ex. `RBX_AMR_PHO_8J_91_007_01.tif`
- **Impact de l'indicateur** : 96 clés (98.0%) suivent la règle directe — ex. `RBX_AMR_PHO_8J_52_000.tif` → `RBX_AMR_PHO_8J_52_ZA_000.tif`
  - 2 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_PHO_8J_91_007_01.tif`)
  - 98 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_9FI — 879 clés — 1 - règle directe

- **Extensions** : tif 879
- **Rôles** : ZA 879
- **Préfixe commun** : `RBX_AMR_DIA_9FI` ; **numéro de vue** : N5 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_DIA_9FI_N5` — 879 (100%) — ex. `RBX_AMR_DIA_9FI_00001.tif`
- **Impact de l'indicateur** : 879 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_DIA_9FI_00001.tif` → `RBX_AMR_DIA_9FI_ZA_00001.tif`
  - 879 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_AFF — 1 098 clés — 1 - règle directe

- **Extensions** : tif 1098
- **Rôles** : ZA 1098
- **Préfixe commun** : `RBX_AMR_AFF` ; **numéro de vue** : N1/N4 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_AFF_N3_<L>_N4_N1` — 1 081 (98%) — ex. `RBX_AMR_AFF_001_D_0001_1.tif`
  - `RBX_AMR_AFF_N3_<L>_N4` — 17 (2%) — ex. `RBX_AMR_AFF_001_D_0973.tif`
- **Impact de l'indicateur** : 1 098 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_AFF_001_D_0001_1.tif` → `RBX_AMR_AFF_001_D_0001_ZA_1.tif`
  - 17 nom(s) avec une lettre avant la vue (D 17 ; ex. `RBX_AMR_AFF_001_D_0973.tif` → `RBX_AMR_AFF_001_D_ZA_0973.tif`)
  - 1098 objet(s) déjà sur S3 à renommer ; 1005 publié(s)

## AMR_AVI — 159 clés — 4 - règle propre au corpus nécessaire

- **Extensions** : mp4 158, mov 1
- **Rôles** : ZD 159
- **Préfixe commun** : `RBX_AMR_AVI` ; **numéro de vue** : - ; **3 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_AVI_#VM#` — 148 (93%) — ex. `RBX_AMR_AVI_0676VM0007.mp4`
  - `RBX_AMR_AVI_#FT#` — 6 (4%) — ex. `RBX_AMR_AVI_0676FT0004.mp4`
  - `RBX_AMR_AVI_#FS#` — 5 (3%) — ex. `RBX_AMR_AVI_0676FS0001.mp4`
- **Impact de l'indicateur** : 0 clés (0.0%) suivent la règle directe
  - 159 nom(s) sans numéro final (code_sans_separateur 159 ; ex. `RBX_AMR_AVI_0676FS0001.mp4`)
  - 159 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_CAD — 296 clés — 2 - règle directe + exceptions à lister

- **Extensions** : jpg 244, tif 52
- **Rôles** : ZD 244, ZA 52
- **Préfixe commun** : `RBX_AMR_CAD` ; **numéro de vue** : N2 ; **4 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_CAD_N4_#G#_N1_N2` — 188 (64%) — ex. `RBX_AMR_CAD_1847_1G1002_1_01.jpg`
  - `RBX_AMR_CAD_N4_#G#_N2` — 87 (29%) — ex. `RBX_AMR_CAD_1804_1G0007_01.jpg`
  - `RBX_<SERIE>_<SERIE>_N4_#G#_N2` — 20 (7%) — ex. `RBX_AMR_PLA_1819_1G1001_01.jpg`
  - … 1 autre(s) motif(s)  1 clés
- **Impact de l'indicateur** : 295 clés (99.7%) suivent la règle directe — ex. `RBX_AMR_CAD_1804_1G0007_01.jpg` → `RBX_AMR_CAD_1804_1G0007_ZD_01.jpg`
  - 1 nom(s) sans numéro final (autre 1 ; ex. `RBX_AMR_CAD_1884_1G1028_TA.jpg`)
  - 193 image(s) de diffusion sans master de même nom
  - 102 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 296 objet(s) déjà sur S3 à renommer ; 296 publié(s)

## AMR_CIM — 16 070 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 16070
- **Rôles** : ZA 16070
- **Préfixe commun** : `RBX_AMR_CIM_CPE` ; **numéro de vue** : N3 ; **8 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_CIM_CPE_N8_N3` — 9 178 (57%) — ex. `RBX_AMR_CIM_CPE_18500329_000.tif`
  - `RBX_AMR_CIM_CPE_N8_<L>_N3` — 6 784 (42%) — ex. `RBX_AMR_CIM_CPE_18530804_A_000.tif`
  - `RBX_<SERIE>_<SERIE>_<SERIE>_<L>_N3` — 95 (1%) — ex. `RBX_AMR_CIM_RCP_A_001.tif`
  - … 5 autre(s) motif(s)  13 clés
- **Impact de l'indicateur** : 16 060 clés (99.9%) suivent la règle directe — ex. `RBX_AMR_CIM_CPE_18500329_000.tif` → `RBX_AMR_CIM_CPE_18500329_ZA_000.tif`
  - 10 nom(s) sans numéro final (bis_ou_lettre_collee 10 ; ex. `RBX_AMR_CIM_CPE_18650819_005a.tif`)
  - 6879 nom(s) avec une lettre avant la vue (A 2411, B 2292, C 823, D 492, E 315, F 204 ; ex. `RBX_AMR_CIM_CPE_18530804_A_000.tif` → `RBX_AMR_CIM_CPE_18530804_A_ZA_000.tif`)
  - 1 nom(s) avec un segment Z existant (ex. `RBX_AMR_CIM_RCP_Z_001.tif`)
  - 1 extension(s) en majuscules
  - 16070 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_DEL — 169 733 clés — 2 - règle directe + exceptions à lister

- **Extensions** : xml 78411, tif 67366, jpg 23676, pdf 140, txt 140
- **Rôles** : ZO 78551, ZA 67366, ZD 23816
- **Préfixe commun** : `RBX_AMR_DEL` ; **numéro de vue** : N4/N8/N1 ; **6 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_DEL_#D#_N4_N4` — 109 060 (64%) — ex. `RBX_AMR_DEL_1D008_0001_0001.tif`
  - `RBX_AMR_DEL_#W#_N8_N4` — 47 352 (28%) — ex. `RBX_AMR_DEL_499W001_19830221_0001.jpg`
  - `RBX_AMR_DEL_#D#_N4` — 11 937 (7%) — ex. `RBX_AMR_DEL_1D001_0001.tif`
  - … 3 autre(s) motif(s)  1 384 clés
- **Impact de l'indicateur** : 169 450 clés (99.8%) suivent la règle directe — ex. `RBX_AMR_DEL_1D001_0001.tif` → `RBX_AMR_DEL_1D001_ZA_0001.tif`
  - 282 fichier(s) de document sans numéro de vue (ex. `RBX_AMR_DEL_1D225_1972_0022.tif`)
  - 1 sous-vue(s) (ex. `RBX_AMR_DEL_1D225_1972_0022_1.tif`)
  - 23676 image(s) de diffusion sans master de même nom
  - 157102 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 165051 objet(s) déjà sur S3 à renommer ; 122098 publié(s)

## AMR_EC — 260 628 clés — 2 - règle directe + exceptions à lister

- **Extensions** : jpg 260617, txt 11
- **Rôles** : ZD 260617, ZO 11
- **Préfixe commun** : `(aucun)` ; **numéro de vue** : N4/N3/N2 ; **153 motif(s)**
- **Règle de nommage observée** :
  - `<SERIE>_<SERIE>_N3_N4` — 60 954 (23%) — ex. `Reg_EC_001_0001.jpg`
  - `<SERIE>_<SERIE>_N2_N4` — 46 774 (18%) — ex. `MI_EC_01_0001.jpg`
  - `D#_N2_N3` — 44 380 (17%) — ex. `D1908_01_001.jpg`
  - … 150 autre(s) motif(s)  108 520 clés
- **Impact de l'indicateur** : 260 432 clés (99.9%) suivent la règle directe — ex. `N1913_03_001.jpg` → `N1913_03_ZD_001.jpg`
  - 196 nom(s) sans numéro final (autre 186, bis_ou_lettre_collee 6, code_sans_separateur 2, intervalle 2 ; ex. `00.jpg`)
  - 17028 nom(s) avec une lettre avant la vue (T 9442, A 2866, D 937, B 871, V 450, L 431 ; ex. `D1995_01_A_001.jpg` → `D1995_01_A_ZD_001.jpg`)
  - 14 nom(s) avec un segment Z existant (ex. `TD1933_1942_Z_001.jpg`)
  - 260617 image(s) de diffusion sans master de même nom
  - 1223 extension(s) en majuscules
  - 21750 nom(s) avec espace, parenthèse ou point
  - 260628 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_GUE — 1 022 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : jpg 341, xml 341, tif 340
- **Rôles** : ZD 341, ZO 341, ZA 340
- **Préfixe commun** : `RBX_AMR_AFF_7H` ; **numéro de vue** : N4/N2/N1 ; **4 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_AFF_7H_N4` — 632 (62%) — ex. `RBX_AMR_AFF_7H_0001.jpg`
  - `RBX_AMR_AFF_7H_N4_N2_N2` — 228 (22%) — ex. `RBX_AMR_AFF_7H_0061_02_01.jpg`
  - `RBX_AMR_AFF_7H_N4_N2` — 156 (15%) — ex. `RBX_AMR_AFF_7H_0011_01.jpg`
  - … 1 autre(s) motif(s)  6 clés
- **Impact de l'indicateur** : 914 clés (89.4%) suivent la règle directe — ex. `RBX_AMR_AFF_7H_0001.jpg` → `RBX_AMR_AFF_7H_ZD_0001.jpg`
  - 108 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_AFF_7H_0011_01.jpg`)
  - 1 image(s) de diffusion sans master de même nom
  - 1022 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 340 objet(s) déjà sur S3 à renommer ; 682 publié(s)

## AMR_OBJ — 100 clés — 1 - règle directe

- **Extensions** : tif 100
- **Rôles** : ZA 100
- **Préfixe commun** : `RBX_AMR_Obj` ; **numéro de vue** : N2 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_Obj_N3_N2` — 99 (99%) — ex. `RBX_AMR_Obj_002_01.tif`
  - `RBX_<SERIE>_<SERIE>_N3_N2` — 1 (1%) — ex. `RBX_AMR_OBJ_042_01.tif`
- **Impact de l'indicateur** : 100 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_OBJ_042_01.tif` → `RBX_AMR_OBJ_042_ZA_01.tif`
  - 100 objet(s) déjà sur S3 à renommer ; 99 publié(s)

## AMR_PAR — 245 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : tif 232, jpg 13
- **Rôles** : ZA 232, ZD 13
- **Préfixe commun** : `RBX_AMR_PAR_LAB` ; **numéro de vue** : N2/N3/N4 ; **8 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PAR_LAB_PH#_N3_N2` — 205 (84%) — ex. `RBX_AMR_PAR_LAB_PH02_005_02.tif`
  - `RBX_AMR_PAR_LAB_PH#_N3` — 21 (9%) — ex. `RBX_AMR_PAR_LAB_PH02_050.jpg`
  - `RBX_<SERIE>_<SERIE>_<SERIE>_D#_N3_N3` — 14 (6%) — ex. `RBX_AMR_PAR_LES_D01_006_027.tif`
  - … 5 autre(s) motif(s)  5 clés
- **Impact de l'indicateur** : 214 clés (87.3%) suivent la règle directe — ex. `RBX_AMR_PAR_LAB_OBJ_012_02.JPG` → `RBX_AMR_PAR_LAB_OBJ_012_ZD_02.JPG`
  - 14 fichier(s) de document sans numéro de vue (ex. `RBX_AMR_PAR_LAB_PH02_050.jpg`)
  - 15 sous-vue(s) (ex. `RBX_AMR_PAR_LAB_PH02_090_01.tif`)
  - 2 nom(s) sans numéro final (copie_(n) 2 ; ex. `RBX_AMR_PAR_LAB_PH05_113 (2).tif`)
  - 13 image(s) de diffusion sans master de même nom
  - 1 extension(s) en majuscules
  - 2 nom(s) avec espace, parenthèse ou point
  - 245 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_PHO — 1 266 clés — 1 - règle directe

- **Extensions** : tif 1266
- **Rôles** : ZA 1266
- **Préfixe commun** : `RBX_AMR_PHO_COM_AMR1` ; **numéro de vue** : N4 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PHO_COM_AMR1_N4` — 1 266 (100%) — ex. `RBX_AMR_PHO_COM_AMR1_0001.tif`
- **Impact de l'indicateur** : 1 266 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_PHO_COM_AMR1_0001.tif` → `RBX_AMR_PHO_COM_AMR1_ZA_0001.tif`
  - 1266 objet(s) déjà sur S3 à renommer ; 1194 publié(s)

## AMR_PLA — 244 clés — 4 - règle propre au corpus nécessaire

- **Extensions** : tif 126, jpg 118
- **Rôles** : ZA 126, ZD 118
- **Préfixe commun** : `RBX_AMR_PLA` ; **numéro de vue** : N2/N3 ; **6 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PLA_#Fi#_N2` — 138 (57%) — ex. `RBX_AMR_PLA_6Fi022_01.tif`
  - `RBX_AMR_PLA_#Fi#` — 79 (32%) — ex. `RBX_AMR_PLA_6Fi005.tif`
  - `RBX_AMR_PLA_#G#_N2` — 20 (8%) — ex. `RBX_AMR_PLA_1G1001_10.tif`
  - … 3 autre(s) motif(s)  7 clés
- **Impact de l'indicateur** : 159 clés (65.2%) suivent la règle directe — ex. `RBX_AMR_PLA_6Fi022_01.tif` → `RBX_AMR_PLA_6Fi022_ZA_01.tif`
  - 4 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_PLA_6Fi038_01_01.jpg`)
  - 81 nom(s) sans numéro final (autre 79, code_sans_separateur 2 ; ex. `RBX_AMR_PLA_6Fi005.tif`)
  - 12 image(s) de diffusion sans master de même nom
  - 212 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 239 objet(s) déjà sur S3 à renommer ; 244 publié(s)

## AMR_POP — 93 576 clés — 1 - règle directe

- **Extensions** : jpg 85436, tif 8127, tiff 8, txt 4, pdf 1
- **Rôles** : ZD 85437, ZA 8135, ZO 4
- **Préfixe commun** : `RBX_AMR_POP` ; **numéro de vue** : N3/N4 ; **4 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_POP_N4_N2_N3` — 77 020 (82%) — ex. `RBX_AMR_POP_1799_01_001.jpg`
  - `RBX_AMR_POP_N4_N2_N4` — 8 428 (9%) — ex. `RBX_AMR_POP_1846_01_1000.jpg`
  - `RBX_AMR_POP_N4_N3_N3` — 8 127 (9%) — ex. `RBX_AMR_POP_1982_001_000.tif`
  - … 1 autre(s) motif(s)  1 clés
- **Impact de l'indicateur** : 93 576 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_POP_1799_01_001.jpg` → `RBX_AMR_POP_1799_01_ZD_001.jpg`
  - 85428 image(s) de diffusion sans master de même nom
  - 18 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 15 extension(s) en majuscules
  - 85449 objet(s) déjà sur S3 à renommer ; 65107 publié(s)

## AMR_PR — 4 199 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 3533, jpg 660, xml 6
- **Rôles** : ZA 3533, ZD 660, ZO 6
- **Préfixe commun** : `RBX_AMR_PR` ; **numéro de vue** : N4/N3/N2 ; **12 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PR_<SERIE>_N3_N4` — 2 983 (71%) — ex. `RBX_AMR_PR_COR_001_0001.tif`
  - `RBX_AMR_PR_<SERIE>_N3_N3` — 616 (15%) — ex. `RBX_AMR_PR_FLO_006_001.jpg`
  - `RBX_AMR_PR_<SERIE>_N3` — 380 (9%) — ex. `RBX_AMR_PR_CAN_001.tif`
  - … 9 autre(s) motif(s)  220 clés
- **Impact de l'indicateur** : 4 166 clés (99.2%) suivent la règle directe — ex. `RBX_AMR_BAR_5M143_nc01_01.jpg` → `RBX_AMR_BAR_5M143_nc01_ZD_01.jpg`
  - 6 fichier(s) de document sans numéro de vue (ex. `RBX_AMR_PR_COR_001.xml`)
  - 25 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_PR_HFR_036_2.tif`)
  - 2 nom(s) sans numéro final (bis_ou_lettre_collee 2 ; ex. `RBX_AMR_PR_CAN_014a.tif`)
  - 660 image(s) de diffusion sans master de même nom
  - 4199 objet(s) déjà sur S3 à renommer ; 658 publié(s)

## AMR_PUV — 9 733 clés — 2 - règle directe + exceptions à lister

- **Extensions** : jpg 3151, tif 3151, xml 3151, pdf 280
- **Rôles** : ZD 3431, ZA 3151, ZO 3151
- **Préfixe commun** : `RBX_AMR_PUV1` ; **numéro de vue** : N3/N2 ; **6 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PUV1_N4_N2_N3` — 9 387 (96%) — ex. `RBX_AMR_PUV1_1965_02_001.jpg`
  - `RBX_AMR_PUV1_N4_N2` — 264 (3%) — ex. `RBX_AMR_PUV1_2014_10.pdf`
  - `RBX_AMR_PUV1_N4_#B_N3` — 66 (1%) — ex. `RBX_AMR_PUV1_1982_12B_001.jpg`
  - … 3 autre(s) motif(s)  16 clés
- **Impact de l'indicateur** : 9 537 clés (98.0%) suivent la règle directe — ex. `RBX_AMR_PUV1_1965_02_001.jpg` → `RBX_AMR_PUV1_1965_02_ZD_001.jpg`
  - 180 fichier(s) de document sans numéro de vue (ex. `RBX_AMR_PUV1_2014_10.pdf`)
  - 16 nom(s) sans numéro final (intervalle 13, autre 3 ; ex. `RBX_AMR_PUV1_2015_06-08.pdf`)
  - 9453 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 9733 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_PVC — 19 170 clés — 2 - règle directe + exceptions à lister

- **Extensions** : jpg 9444, xml 9444, pdf 141, txt 141
- **Rôles** : ZD 9585, ZO 9585
- **Préfixe commun** : `RBX_AMR_PVC` ; **numéro de vue** : N4/N8 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_PVC_#W#_N8_N4` — 18 888 (99%) — ex. `RBX_AMR_PVC_246W042_19830221_0001.jpg`
  - `RBX_AMR_PVC_#W#_N8` — 282 (1%) — ex. `RBX_AMR_PVC_246W042_19830221.pdf`
- **Impact de l'indicateur** : 18 888 clés (98.5%) suivent la règle directe — ex. `RBX_AMR_PVC_246W042_19830221.pdf` → `RBX_AMR_PVC_246W042_ZD_19830221.pdf`
  - 282 fichier(s) de document sans numéro de vue (ex. `RBX_AMR_PVC_246W042_19830221.pdf`)
  - 9444 image(s) de diffusion sans master de même nom
  - 19170 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 19170 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## AMR_RAM — 36 843 clés — 2 - règle directe + exceptions à lister

- **Extensions** : xml 18409, tif 18408, pdf 26
- **Rôles** : ZO 18409, ZA 18408, ZD 26
- **Préfixe commun** : `RBX_AMR_RAM` ; **numéro de vue** : N4 ; **4 motif(s)**
- **Règle de nommage observée** :
  - `RBX_AMR_RAM_#D#_N4_N4` — 35 175 (95%) — ex. `RBX_AMR_RAM_3D001_1862_0001.tif`
  - `RBX_AMR_RAM_#D#_#-#_N4` — 1 642 (4%) — ex. `RBX_AMR_RAM_3D020_1914-1930_0001.tif`
  - `RBX_<SERIE>_#D_N3_#-#` — 15 (0%) — ex. `RBX_AMR_3D_001_1862-1870.pdf`
  - … 1 autre(s) motif(s)  11 clés
- **Impact de l'indicateur** : 36 828 clés (100.0%) suivent la règle directe — ex. `RBX_AMR_3D_014_1908.pdf` → `RBX_AMR_3D_014_ZD_1908.pdf`
  - 15 nom(s) sans numéro final (intervalle 15 ; ex. `RBX_AMR_3D_001_1862-1870.pdf`)
  - 36816 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 36843 objet(s) déjà sur S3 à renommer ; 36817 publié(s)

## ARA_CPS — 174 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : jpg 94, wav 80
- **Rôles** : ZD 94, ZA 80
- **Préfixe commun** : `RBX_ARA_CPS_VI2` ; **numéro de vue** : N3 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_ARA_CPS_VI2_<SERIE>_N3` — 146 (84%) — ex. `RBX_ARA_CPS_VI2_HOR_001.jpg`
  - `RBX_ARA_CPS_VI2_<SERIE>_N3_N3` — 28 (16%) — ex. `RBX_ARA_CPS_VI2_PAT_001_001.jpg`
- **Impact de l'indicateur** : 140 clés (80.5%) suivent la règle directe — ex. `RBX_ARA_CPS_VI2_HOR_001.jpg` → `RBX_ARA_CPS_VI2_HOR_ZD_001.jpg`
  - 10 fichier(s) de document sans numéro de vue (ex. `RBX_ARA_CPS_VI2_PAT_001.wav`)
  - 24 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_ARA_CPS_VI2_PAT_001_001.jpg`)
  - 27 image(s) de diffusion sans master de même nom
  - 134 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 174 objet(s) déjà sur S3 à renommer ; 174 publié(s)

## CSV_PAL — 242 clés — 1 - règle directe

- **Extensions** : tif 242
- **Rôles** : ZA 242
- **Préfixe commun** : `RBX_CSV_PAL` ; **numéro de vue** : N2 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_CSV_PAL_N4_N2` — 242 (100%) — ex. `RBX_CSV_PAL_1858_01.tif`
- **Impact de l'indicateur** : 242 clés (100.0%) suivent la règle directe — ex. `RBX_CSV_PAL_1858_01.tif` → `RBX_CSV_PAL_1858_ZA_01.tif`
  - 242 objet(s) déjà sur S3 à renommer ; 242 publié(s)

## LAI — 1 661 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : tif 1661
- **Rôles** : ZA 1661
- **Préfixe commun** : `RBX` ; **numéro de vue** : N3 ; **3 motif(s)**
- **Règle de nommage observée** :
  - `RBX_<SERIE>_D#_N3_N3` — 1 145 (69%) — ex. `RBX_LAI_D01_001_001.tif`
  - `RBX_<SERIE>_<SERIE>_D#_N3` — 416 (25%) — ex. `RBX_MED_LAI_D04_002.tif`
  - `RBX_<SERIE>_<SERIE>_D#_N3_N3` — 100 (6%) — ex. `RBX_MED_LAI_D04_002_001.tif`
- **Impact de l'indicateur** : 1 561 clés (94.0%) suivent la règle directe — ex. `RBX_LAI_D01_001_001.tif` → `RBX_LAI_D01_001_ZA_001.tif`
  - 100 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_MED_LAI_D04_002_001.tif`)
  - 1661 objet(s) déjà sur S3 à renommer ; 1145 publié(s)

## LAR_PUB — 3 731 clés — 1 - règle directe

- **Extensions** : tif 3731
- **Rôles** : ZA 3731
- **Préfixe commun** : `RBX_LAR_PUB` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_LAR_PUB_<SERIE>_N3_N3` — 3 731 (100%) — ex. `RBX_LAR_PUB_AMM_001_000.tif`
- **Impact de l'indicateur** : 3 731 clés (100.0%) suivent la règle directe — ex. `RBX_LAR_PUB_AMM_001_000.tif` → `RBX_LAR_PUB_AMM_001_ZA_000.tif`
  - 3731 objet(s) déjà sur S3 à renommer ; 3731 publié(s)

## MDF_MTX — 18 clés — 4 - règle propre au corpus nécessaire

- **Extensions** : mov 18
- **Rôles** : ZD 18
- **Préfixe commun** : `RBX_MDF_MTX` ; **numéro de vue** : - ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MDF_MTX_N4_D#` — 18 (100%) — ex. `RBX_MDF_MTX_2012_D02.mov`
- **Impact de l'indicateur** : 0 clés (0.0%) suivent la règle directe
  - 18 nom(s) sans numéro final (code_sans_separateur 18 ; ex. `RBX_MDF_MTX_2012_D02.mov`)
  - 18 objet(s) déjà sur S3 à renommer ; 17 publié(s)

## MED_AFF — 4 222 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 4222
- **Rôles** : ZA 4222
- **Préfixe commun** : `RBX_MED_AFF` ; **numéro de vue** : N3 ; **4 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_AFF_N3_<SERIE>_<L>_N4_N3` — 4 047 (96%) — ex. `RBX_MED_AFF_001_LOC_A_0065_001.tif`
  - `RBX_MED_AFF_N3_<L>_N1_N3` — 99 (2%) — ex. `RBX_MED_AFF_001_B_1_001.tif`
  - `RBX_MED_AFF_N3_<L>_N3` — 75 (2%) — ex. `RBX_MED_AFF_001_A_001.tif`
  - … 1 autre(s) motif(s)  1 clés
- **Impact de l'indicateur** : 4 221 clés (100.0%) suivent la règle directe — ex. `RBX_MED_AFF_001_A_001.tif` → `RBX_MED_AFF_001_A_ZA_001.tif`
  - 1 nom(s) sans numéro final (bis_ou_lettre_collee 1 ; ex. `RBX_MED_AFF_001_A_043bis.tif`)
  - 75 nom(s) avec une lettre avant la vue (A 75 ; ex. `RBX_MED_AFF_001_A_001.tif` → `RBX_MED_AFF_001_A_ZA_001.tif`)
  - 4222 objet(s) déjà sur S3 à renommer ; 4119 publié(s)

## MED_AVI — 31 clés — 4 - règle propre au corpus nécessaire

- **Extensions** : mp4 31
- **Rôles** : ZD 31
- **Préfixe commun** : `RBX_MED_AVI` ; **numéro de vue** : - ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_AVI_#VM#` — 31 (100%) — ex. `RBX_MED_AVI_0932VM0001.mp4`
- **Impact de l'indicateur** : 0 clés (0.0%) suivent la règle directe
  - 31 nom(s) sans numéro final (code_sans_separateur 31 ; ex. `RBX_MED_AVI_0932VM0001.mp4`)
  - 31 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## MED_CHA — 363 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : tif 363
- **Rôles** : ZA 363
- **Préfixe commun** : `RBX_MED_CHA` ; **numéro de vue** : N4/N3 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_CHA_N4` — 295 (81%) — ex. `RBX_MED_CHA_0001.tif`
  - `RBX_MED_CHA_N4_N3` — 68 (19%) — ex. `RBX_MED_CHA_0011_001.tif`
- **Impact de l'indicateur** : 293 clés (80.7%) suivent la règle directe — ex. `RBX_MED_CHA_0001.tif` → `RBX_MED_CHA_ZA_0001.tif`
  - 2 fichier(s) de document sans numéro de vue (ex. `RBX_MED_CHA_0011.tif`)
  - 4 sous-vue(s) (ex. `RBX_MED_CHA_0011_001.tif`)
  - 64 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_MED_CHA_0034_001.tif`)
  - 363 objet(s) déjà sur S3 à renommer ; 360 publié(s)

## MED_CP — 5 318 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 5123, jpg 140, jpeg 55
- **Rôles** : ZA 5123, ZD 195
- **Préfixe commun** : `RBX_MED_CP` ; **numéro de vue** : N3/N1 ; **8 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_CP_A#_L#_S#_N3` — 4 830 (91%) — ex. `RBX_MED_CP_A01_L1_S1_001.tif`
  - `RBX_MED_CP_A#_L#_S#_N1_N3` — 268 (5%) — ex. `RBX_MED_CP_A03_L1_S1_1_001.tif`
  - `RBX_MED_CP_A#_L#_S#_<L>_N3` — 164 (3%) — ex. `RBX_MED_CP_A01_L1_S3_A_007.tif`
  - … 5 autre(s) motif(s)  56 clés
- **Impact de l'indicateur** : 5 264 clés (99.0%) suivent la règle directe — ex. `RBX_MED_CP_A01_L1_S1_001.tif` → `RBX_MED_CP_A01_L1_S1_ZA_001.tif`
  - 1 fichier(s) de document sans numéro de vue (ex. `RBX_MED_CP_A24_L1_S1_115.tif`)
  - 1 sous-vue(s) (ex. `RBX_MED_CP_A24_L1_S1_115_1.tif`)
  - 17 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_MED_CP_A06_L1_S1_A_010_1.tif`)
  - 35 nom(s) sans numéro final (autre 35 ; ex. `RBX_MED_CP_A13_L1_S1_014.tif.tif`)
  - 164 nom(s) avec une lettre avant la vue (A 110, B 54 ; ex. `RBX_MED_CP_A01_L1_S3_A_007.tif` → `RBX_MED_CP_A01_L1_S3_A_ZA_007.tif`)
  - 187 image(s) de diffusion sans master de même nom
  - 16 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 35 nom(s) avec espace, parenthèse ou point
  - 5318 objet(s) déjà sur S3 à renommer ; 4663 publié(s)

## MED_EPH — 846 clés — 1 - règle directe

- **Extensions** : tiff 846
- **Rôles** : ZA 846
- **Préfixe commun** : `RBX_MED_EPH` ; **numéro de vue** : N2 ; **5 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_EPH_I#_L#_N3_N2_N2` — 273 (32%) — ex. `RBX_MED_EPH_I1_L1_001_01_01.tiff`
  - `RBX_MED_EPH_D#_L#_N3_N2_N2` — 189 (22%) — ex. `RBX_MED_EPH_D1_L1_001_01_01.tiff`
  - `RBX_MED_EPH_C#_L#_N3_N2_N2` — 149 (18%) — ex. `RBX_MED_EPH_C1_L1_001_01_01.tiff`
  - … 2 autre(s) motif(s)  235 clés
- **Impact de l'indicateur** : 846 clés (100.0%) suivent la règle directe — ex. `RBX_MED_EPH_C1_L1_001_01_01.tiff` → `RBX_MED_EPH_C1_L1_001_01_ZA_01.tiff`
  - 846 objet(s) déjà sur S3 à renommer ; 840 publié(s)

## MED_FLR — 8 487 clés — 4 - règle propre au corpus nécessaire

- **Extensions** : wav 6695, tif 781, mp3 533, jpg 478
- **Rôles** : ZA 7476, ZD 1011
- **Préfixe commun** : `RBX_MED` ; **numéro de vue** : N2/N1 ; **53 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_<SERIE>_<SERIE>_N4_N2_N2` — 3 525 (42%) — ex. `RBX_MED_FLR_DC_1051_01_01.wav`
  - `RBX_MED_<SERIE>_N2_N4_<L>_N2_#kHz#B` — 2 088 (25%) — ex. `RBX_MED_DES_25_0147_A_01_44kHz24B.wav`
  - `RBX_MED_<SERIE>_N2_N4_<L>_N2_<SERIE>` — 1 117 (13%) — ex. `RBX_MED_FLR_17_0094_A_01_TI.wav`
  - … 50 autre(s) motif(s)  1 757 clés
- **Impact de l'indicateur** : 4 351 clés (51.3%) suivent la règle directe — ex. `RBX_MED_FLRS_DES_25_0147_3.tif` → `RBX_MED_FLRS_DES_25_0147_ZA_3.tif`
  - 4136 nom(s) sans numéro final (audio_suffixe_technique 2394, autre 1733, lettre_finale 8, intervalle 1 ; ex. `Discour-P1_P2_brut_44kHz16B.wav`)
  - 45 nom(s) avec une lettre avant la vue (T 43, A 1, B 1 ; ex. `RBX_MED_FLR_17_0922_A_01.wav` → `RBX_MED_FLR_17_0922_A_ZA_01.wav`)
  - 478 image(s) de diffusion sans master de même nom
  - 948 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 6 nom(s) avec espace, parenthèse ou point
  - 8487 objet(s) déjà sur S3 à renommer ; 3585 publié(s)

## MED_FOO — 6 clés — 1 - règle directe

- **Extensions** : mp3 3, jpg 3
- **Rôles** : ZD 6
- **Préfixe commun** : `RBX_MED_FOO_S01` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_FOO_S01_D#_N3` — 6 (100%) — ex. `RBX_MED_FOO_S01_D01_001.mp3`
- **Impact de l'indicateur** : 6 clés (100.0%) suivent la règle directe — ex. `RBX_MED_FOO_S01_D01_001.mp3` → `RBX_MED_FOO_S01_D01_ZD_001.mp3`
  - 3 image(s) de diffusion sans master de même nom
  - 2 extension(s) en majuscules
  - 6 objet(s) déjà sur S3 à renommer ; 6 publié(s)

## MED_IMA — 5 821 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 5641, jpg 180
- **Rôles** : ZA 5641, ZD 180
- **Préfixe commun** : `RBX_MED_IMA` ; **numéro de vue** : N3/N2 ; **7 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_IMA_CI#_L#_D#_N2` — 2 675 (46%) — ex. `RBX_MED_IMA_CI01_L1_D001_01.tif`
  - `RBX_MED_IMA_<SERIE>_<L>_N3_N3` — 1 776 (31%) — ex. `RBX_MED_IMA_CAL_A_001_001.jpg`
  - `RBX_MED_IMA_ETI#_<SERIE>_N3` — 637 (11%) — ex. `RBX_MED_IMA_ETI1_ADO_001.tif`
  - … 4 autre(s) motif(s)  733 clés
- **Impact de l'indicateur** : 5 820 clés (100.0%) suivent la règle directe — ex. `RBX_MED_IMA_BUV1_001.jpg` → `RBX_MED_IMA_BUV1_ZD_001.jpg`
  - 1 nom(s) sans numéro final (autre 1 ; ex. `RBX_MED_IMA_BUV1_063..tif`)
  - 1 image(s) de diffusion sans master de même nom
  - 358 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 1 nom(s) avec espace, parenthèse ou point
  - 5821 objet(s) déjà sur S3 à renommer ; 5767 publié(s)

## MED_JOU — 1 995 clés — 1 - règle directe

- **Extensions** : tif 1995
- **Rôles** : ZA 1995
- **Préfixe commun** : `RBX_MED_JOU` ; **numéro de vue** : N3/N2 ; **3 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_JOU_P#_N4_N3` — 1 295 (65%) — ex. `RBX_MED_JOU_P20001_1932_000.tif`
  - `RBX_MED_JOU_P#_n#_N4_N2` — 694 (35%) — ex. `RBX_MED_JOU_P50028_n100_1884_01.tif`
  - `RBX_MED_JOU_P#_n#a_N4_N2` — 6 (0%) — ex. `RBX_MED_JOU_P50028_n163a_1885_01.tif`
- **Impact de l'indicateur** : 1 995 clés (100.0%) suivent la règle directe — ex. `RBX_MED_JOU_P20001_1932_000.tif` → `RBX_MED_JOU_P20001_1932_ZA_000.tif`
  - 1995 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## MED_LET — 604 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : tif 604
- **Rôles** : ZA 604
- **Préfixe commun** : `RBX_MED_LET` ; **numéro de vue** : N4 ; **3 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_LET_N4` — 570 (94%) — ex. `RBX_MED_LET_0001.tif`
  - `RBX_MED_LET_#bis` — 32 (5%) — ex. `RBX_MED_LET_0010bis.tif`
  - `RBX_MED_LET_#bis.c` — 2 (0%) — ex. `RBX_MED_LET_0056bis.c.tif`
- **Impact de l'indicateur** : 570 clés (94.4%) suivent la règle directe — ex. `RBX_MED_LET_0001.tif` → `RBX_MED_LET_ZA_0001.tif`
  - 34 nom(s) sans numéro final (bis_ou_lettre_collee 32, autre 2 ; ex. `RBX_MED_LET_0010bis.tif`)
  - 2 nom(s) avec espace, parenthèse ou point
  - 604 objet(s) déjà sur S3 à renommer ; 559 publié(s)

## MED_MAR — 99 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 99
- **Rôles** : ZA 99
- **Préfixe commun** : `RBX_MED_MAR` ; **numéro de vue** : N3 ; **3 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_MAR_N3_<L>_N3_N3` — 89 (90%) — ex. `RBX_MED_MAR_001_B_004_001.tif`
  - `RBX_MED_MAR_<SERIE>_AM#_<L>_N3_N3` — 9 (9%) — ex. `RBX_MED_MAR_AFF_AM01_B_012_001.tif`
  - `RBX_MED_MAR_N3_<L>_N3_# - dans aucun train CADN # ou #` — 1 (1%) — ex. `RBX_MED_MAR_014_A_001_001 - dans aucun train CADN 2019 ou 2021.tif`
- **Impact de l'indicateur** : 98 clés (99.0%) suivent la règle directe — ex. `RBX_MED_MAR_001_B_004_001.tif` → `RBX_MED_MAR_001_B_004_ZA_001.tif`
  - 1 nom(s) sans numéro final (autre 1 ; ex. `RBX_MED_MAR_014_A_001_001 - dans aucun train CADN 2019 ou 2021.tif`)
  - 1 nom(s) avec espace, parenthèse ou point
  - 99 objet(s) déjà sur S3 à renommer ; 62 publié(s)

## MED_MON — 7 553 clés — 1 - règle directe

- **Extensions** : tif 4832, xml 2721
- **Rôles** : ZA 4832, ZO 2610, METS 111
- **Préfixe commun** : `RBX_MED_MON` ; **numéro de vue** : N3 ; **9 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_MON_<SERIE>_N6_N3` — 2 281 (30%) — ex. `RBX_MED_MON_FOL_000747_000.tif`
  - `RBX_MED_MON_BR#_N4_N3` — 1 991 (26%) — ex. `RBX_MED_MON_BR8_2709_001.tif`
  - `RBX_MED_MON_N1_N6_N3` — 1 280 (17%) — ex. `RBX_MED_MON_4_210000_001.tif`
  - … 6 autre(s) motif(s)  2 001 clés
- **Impact de l'indicateur** : 7 553 clés (100.0%) suivent la règle directe — ex. `RBX_MED_MON_4_210000_001.tif` → `RBX_MED_MON_4_210000_ZA_001.tif`
  - 111 fichier(s) METS, nom inchangé car déjà suffixé `.mets.xml` (ex. `RBX_MED_MON_4_207931.mets.xml`)
  - 5220 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 7553 objet(s) déjà sur S3 à renommer ; 1384 publié(s)

## MED_MS — 32 414 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 32414
- **Rôles** : ZA 32414
- **Préfixe commun** : `RBX_MED_MS` ; **numéro de vue** : N3/N4 ; **5 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_MS_N3_N3` — 32 042 (99%) — ex. `RBX_MED_MS_009_001.tif`
  - `RBX_MED_MS_N3_N4` — 198 (1%) — ex. `RBX_MED_MS_006_0000.tif`
  - `RBX_MED_MS_N3_N4_<L>` — 170 (1%) — ex. `RBX_MED_MS_007_0000_A.tif`
  - … 2 autre(s) motif(s)  4 clés
- **Impact de l'indicateur** : 32 240 clés (99.5%) suivent la règle directe — ex. `RBX_MED_MS_006_0000.tif` → `RBX_MED_MS_006_ZA_0000.tif`
  - 174 nom(s) sans numéro final (lettre_finale 170, autre 3, bis_ou_lettre_collee 1 ; ex. `RBX_MED_MS_007_0000_A.tif`)
  - 32414 objet(s) déjà sur S3 à renommer ; 32410 publié(s)

## MED_PAR — 18 223 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 17071, jpg 1149, tiff 1, mp4 1, wmv 1
- **Rôles** : ZA 17072, ZD 1151
- **Préfixe commun** : `RBX` ; **numéro de vue** : N3/N2/N4 ; **41 motif(s)**
- **Règle de nommage observée** :
  - `RBX_<SERIE>_<SERIE>_<SERIE>_<SERIE>_N4_N3` — 6 342 (35%) — ex. `RBX_MED_PAR_BOU_JAC_1408_001.tif`
  - `RBX_<SERIE>_<SERIE>_<SERIE>_<SERIE>_D#_N3` — 3 177 (17%) — ex. `RBX_MED_PAR_BOH_RES_D01_001.tif`
  - `RBX_<SERIE>_<SERIE>_<SERIE>_PH#_N3_N2` — 2 877 (16%) — ex. `RBX_AMR_PAR_LAB_PH01_001_01.jpg`
  - … 38 autre(s) motif(s)  5 827 clés
- **Impact de l'indicateur** : 17 589 clés (96.5%) suivent la règle directe — ex. `MED_PAR_CAR_CHO_D01_001.tif` → `MED_PAR_CAR_CHO_D01_ZA_001.tif`
  - 5 fichier(s) de document sans numéro de vue (ex. `RBX_AMR_PAR_LAB_PH02_090.tif`)
  - 17 sous-vue(s) (ex. `RBX_AMR_PAR_LAB_PH02_050_01.tif`)
  - 409 nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `RBX_AMR_PAR_LAB_OBJ_001_01.JPG`)
  - 203 nom(s) sans numéro final (bis_ou_lettre_collee 201, autre 2 ; ex. `RBX_MED_PAR_BOH_RES_D09_009a.tif`)
  - 1146 image(s) de diffusion sans master de même nom
  - 6 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 43 extension(s) en majuscules
  - 18223 objet(s) déjà sur S3 à renommer ; 17448 publié(s)

## MED_PBI — 1 084 clés — 1 - règle directe

- **Extensions** : tif 1084
- **Rôles** : ZA 1084
- **Préfixe commun** : `RBX_MED_PBI_LAB` ; **numéro de vue** : N2/N3 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_PBI_LAB_<SERIE>_<SERIE>_N3_N2` — 1 083 (100%) — ex. `RBX_MED_PBI_LAB_MEL_ANN_001_01.tif`
  - `RBX_MED_PBI_LAB_<SERIE>_<SERIE>_N3_N3` — 1 (0%) — ex. `RBX_MED_PBI_LAB_TRA_TMW_006_011.tif`
- **Impact de l'indicateur** : 1 084 clés (100.0%) suivent la règle directe — ex. `RBX_MED_PBI_LAB_MEL_ANN_001_01.tif` → `RBX_MED_PBI_LAB_MEL_ANN_001_ZA_01.tif`
  - 1084 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## MED_PER — 1 596 clés — 1 - règle directe

- **Extensions** : xml 829, tif 767
- **Rôles** : ZA 767, ZO 765, METS 64
- **Préfixe commun** : `RBX_MED_PER` ; **numéro de vue** : N3 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_PER_P#_N8_N3` — 1 532 (96%) — ex. `RBX_MED_PER_P20475_19020802_001.tif`
  - `RBX_MED_PER_P#_#.mets` — 64 (4%) — ex. `RBX_MED_PER_P20475_19020802.mets.xml`
- **Impact de l'indicateur** : 1 596 clés (100.0%) suivent la règle directe — ex. `RBX_MED_PER_P20475_19020802_001.tif` → `RBX_MED_PER_P20475_19020802_ZA_001.tif`
  - 64 fichier(s) METS, nom inchangé car déjà suffixé `.mets.xml` (ex. `RBX_MED_PER_P20475_19020802.mets.xml`)
  - 1530 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 1596 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## MED_PHD — 1 946 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 1946
- **Rôles** : ZA 1946
- **Préfixe commun** : `RBX_MED_PHD` ; **numéro de vue** : N3 ; **12 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_PHD_URB#_S#_<L>_D#_N3` — 547 (28%) — ex. `RBX_MED_PHD_URB01_S01_A_D01_001.tif`
  - `RBX_MED_PHD_SOC#_S#_D#_N3` — 435 (22%) — ex. `RBX_MED_PHD_SOC01_S01_D01_001.tif`
  - `RBX_MED_PHD_URB#_S#_D#_N3` — 343 (18%) — ex. `RBX_MED_PHD_URB01_S01_D01_001.tif`
  - … 9 autre(s) motif(s)  621 clés
- **Impact de l'indicateur** : 1 945 clés (99.9%) suivent la règle directe — ex. `RBX_MED_PHD_ETA01_S01_A_D01_001.tif` → `RBX_MED_PHD_ETA01_S01_A_D01_ZA_001.tif`
  - 1 nom(s) sans numéro final (autre 1 ; ex. `RBX_MED_PHD_SOC01_S05_D11.001.tif`)
  - 34 nom(s) avec une lettre avant la vue (B 27, A 5, C 2 ; ex. `RBX_MED_PHD_SOC03_S01_D03_B_001.tif` → `RBX_MED_PHD_SOC03_S01_D03_B_ZA_001.tif`)
  - 1 nom(s) avec espace, parenthèse ou point
  - 1946 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## MED_PHO — 1 338 clés — 1 - règle directe

- **Extensions** : tif 1063, jpg 274, png 1
- **Rôles** : ZA 1063, ZD 275
- **Préfixe commun** : `RBX_MED_PHO` ; **numéro de vue** : N3/N2 ; **3 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_PHO_<SERIE>_DEL#_N3` — 917 (69%) — ex. `RBX_MED_PHO_DIA_DEL1_001.tif`
  - `RBX_MED_PHO_<SERIE>_DUP#_D#_N2` — 275 (21%) — ex. `RBX_MED_PHO_PNU_DUP01_D01_01.jpg`
  - `RBX_MED_PHO_<SERIE>_SAU#_N3` — 146 (11%) — ex. `RBX_MED_PHO_DIA_SAU1_001.tif`
- **Impact de l'indicateur** : 1 338 clés (100.0%) suivent la règle directe — ex. `RBX_MED_PHO_DIA_DEL1_001.tif` → `RBX_MED_PHO_DIA_DEL1_ZA_001.tif`
  - 275 image(s) de diffusion sans master de même nom
  - 71 extension(s) en majuscules
  - 1338 objet(s) déjà sur S3 à renommer ; 1140 publié(s)

## MED_PLA — 274 clés — 2 - règle directe + exceptions à lister

- **Extensions** : tif 274
- **Rôles** : ZA 274
- **Préfixe commun** : `RBX_MED_PLA` ; **numéro de vue** : N3 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_PLA_<L>_N3_N3` — 273 (100%) — ex. `RBX_MED_PLA_A_004_001.tif`
  - `RBX_MED_PLA_<L>_N3_N3_<SERIE>_<SERIE>_<SERIE>` — 1 (0%) — ex. `RBX_MED_PLA_A_029_001_tres_haute_def.tif`
- **Impact de l'indicateur** : 273 clés (99.6%) suivent la règle directe — ex. `RBX_MED_PLA_A_004_001.tif` → `RBX_MED_PLA_A_004_ZA_001.tif`
  - 1 nom(s) sans numéro final (autre 1 ; ex. `RBX_MED_PLA_A_029_001_tres_haute_def.tif`)
  - 274 objet(s) déjà sur S3 à renommer ; 271 publié(s)

## MED_PUB — 1 274 clés — 1 - règle directe

- **Extensions** : tif 1274
- **Rôles** : ZA 1274
- **Préfixe commun** : `RBX_MED_PUB` ; **numéro de vue** : N2 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_PUB_<SERIE>_<SERIE>_N3_N2` — 1 137 (89%) — ex. `RBX_MED_PUB_PAT_FDP_001_01.tif`
  - `RBX_MED_PUB_<SERIE>_<SERIE>_N2_N2` — 137 (11%) — ex. `RBX_MED_PUB_ETA_BIB_01_01.tif`
- **Impact de l'indicateur** : 1 274 clés (100.0%) suivent la règle directe — ex. `RBX_MED_PUB_ETA_BIB_01_01.tif` → `RBX_MED_PUB_ETA_BIB_01_ZA_01.tif`
  - 1274 objet(s) déjà sur S3 à renommer ; 1274 publié(s)

## MED_VDM — 38 211 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : tif 38211
- **Rôles** : ZA 38211
- **Préfixe commun** : `RBX_MED_VDM` ; **numéro de vue** : N3/N2/N4 ; **232 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MED_VDM_LFP#_D#_N3` — 5 226 (14%) — ex. `RBX_MED_VDM_LFP1_D01_001.tif`
  - `RBX_MED_VDM_CAR#_D#_N3` — 2 842 (7%) — ex. `RBX_MED_VDM_CAR1_D01_001.tif`
  - `RBX_MED_VDM_I#_D#_N3` — 2 837 (7%) — ex. `RBX_MED_VDM_I141_D01_001.TIF`
  - … 229 autre(s) motif(s)  27 306 clés
- **Impact de l'indicateur** : 34 962 clés (91.5%) suivent la règle directe — ex. `1RBX_MED_VDM_MAI1_D01_001.tif` → `1RBX_MED_VDM_MAI1_D01_ZA_001.tif`
  - 3249 nom(s) sans numéro final (bis_ou_lettre_collee 2758, lettre_finale 409, autre 80, copie_(n) 2 ; ex. `MED_VDM_TEM1_D04_001 (1).tif`)
  - 717 extension(s) en majuscules
  - 81 nom(s) avec espace, parenthèse ou point
  - 38211 objet(s) déjà sur S3 à renommer ; 31272 publié(s)

## MUS_ARC — 318 clés — 3 - règle directe + part notable de cas à trancher

- **Extensions** : jpg 258, tif 60
- **Rôles** : ZD 258, ZA 60
- **Préfixe commun** : `RBX_MUS_ARC` ; **numéro de vue** : N3/N2 ; **11 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MUS_ARC_<SERIE>_D#_N2` — 75 (24%) — ex. `RBX_MUS_ARC_FDT_D01_01.tif`
  - `RBX_MUS_ARC_CAV#_N3` — 70 (22%) — ex. `RBX_MUS_ARC_CAV2_001.jpg`
  - `RBX_MUS_ARC_<SERIE>_N3_N2` — 53 (17%) — ex. `RBX_MUS_ARC_EINF_008_01.tif`
  - … 8 autre(s) motif(s)  120 clés
- **Impact de l'indicateur** : 261 clés (82.1%) suivent la règle directe — ex. `MUS_ARC_GDU3_D011_001.jpg` → `MUS_ARC_GDU3_D011_ZD_001.jpg`
  - 1 fichier(s) de document sans numéro de vue (ex. `RBX_MUS_ARC_EINF_008.tif`)
  - 53 sous-vue(s) (ex. `RBX_MUS_ARC_EINF_008_01.tif`)
  - 3 nom(s) sans numéro final (autre 3 ; ex. `RBX_MUS_ARC_EINF_001B.tif`)
  - 256 image(s) de diffusion sans master de même nom
  - 4 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 1 nom(s) avec espace, parenthèse ou point
  - 318 objet(s) déjà sur S3 à renommer ; 134 publié(s)

## MUS_VAI — 1 918 clés — 1 - règle directe

- **Extensions** : tif 1514, jpg 404
- **Rôles** : ZA 1514, ZD 404
- **Préfixe commun** : `RBX_MUS` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `RBX_MUS_VAI#_D#_F#_N3` — 1 918 (100%) — ex. `RBX_MUS_VAI1_D01_F01_001.jpg`
- **Impact de l'indicateur** : 1 918 clés (100.0%) suivent la règle directe — ex. `RBX_MUS_VAI1_D01_F01_001.jpg` → `RBX_MUS_VAI1_D01_F01_ZD_001.jpg`
  - 404 image(s) de diffusion sans master de même nom
  - 1918 objet(s) déjà sur S3 à renommer ; 1859 publié(s)

## OBS_JOU — 1 757 clés — 2 - règle directe + exceptions à lister

- **Extensions** : jpg 1156, tif 558, pdf 43
- **Rôles** : ZD 1199, ZA 558
- **Préfixe commun** : `RBX_OBS_JOU` ; **numéro de vue** : N3/N4 ; **7 motif(s)**
- **Règle de nommage observée** :
  - `RBX_OBS_JOU_<SERIE>_N3_N3` — 1 565 (89%) — ex. `RBX_OBS_JOU_ENT_009_001.jpg`
  - `RBX_OBS_JOU_<SERIE>_N4_N3` — 115 (7%) — ex. `RBX_OBS_JOU_VPC_0018_001.jpg`
  - `RBX_OBS_JOU_<SERIE>_N3` — 31 (2%) — ex. `RBX_OBS_JOU_QLI_001.pdf`
  - … 4 autre(s) motif(s)  46 clés
- **Impact de l'indicateur** : 1 756 clés (99.9%) suivent la règle directe — ex. `RBX_OBS_JOU_ENT_009_001.jpg` → `RBX_OBS_JOU_ENT_009_ZD_001.jpg`
  - 1 nom(s) sans numéro final (bis_ou_lettre_collee 1 ; ex. `RBX_OBS_JOU_VPC_0036a.pdf`)
  - 1022 image(s) de diffusion sans master de même nom
  - 268 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 1757 objet(s) déjà sur S3 à renommer ; 1202 publié(s)

## PRA_AVE — 176 340 clés — 1 - règle directe

- **Extensions** : pdf 44275, tif 44275, txt 44271, xml 43519
- **Rôles** : ZO 87790, ZD 44275, ZA 44275
- **Préfixe commun** : `PRA_AVE` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `PRA_AVE_N8_N3` — 176 340 (100%) — ex. `PRA_AVE_18880921_001.pdf`
- **Impact de l'indicateur** : 176 340 clés (100.0%) suivent la règle directe — ex. `PRA_AVE_18880921_001.pdf` → `PRA_AVE_18880921_ZD_001.pdf`
  - 176340 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 176340 objet(s) déjà sur S3 à renommer ; 87510 publié(s)

## PRA_BDR — 178 clés — 1 - règle directe

- **Extensions** : tif 178
- **Rôles** : ZA 178
- **Préfixe commun** : `PRA_BDR` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `PRA_BDR_N8_N3` — 178 (100%) — ex. `PRA_BDR_19171117_001.tif`
- **Impact de l'indicateur** : 178 clés (100.0%) suivent la règle directe — ex. `PRA_BDR_19171117_001.tif` → `PRA_BDR_19171117_ZA_001.tif`
  - 178 objet(s) déjà sur S3 à renommer ; 0 publié(s)

## PRA_CRT — 116 806 clés — 1 - règle directe

- **Extensions** : pdf 29202, tif 29202, txt 29202, xml 29200
- **Rôles** : ZO 58402, ZD 29202, ZA 29202
- **Préfixe commun** : `PRA_CRT` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `PRA_CRT_N8_N3` — 116 806 (100%) — ex. `PRA_CRT_19010501_001.pdf`
- **Impact de l'indicateur** : 116 806 clés (100.0%) suivent la règle directe — ex. `PRA_CRT_19010501_001.pdf` → `PRA_CRT_19010501_ZD_001.pdf`
  - 116806 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 116806 objet(s) déjà sur S3 à renommer ; 48452 publié(s)

## PRA_CTG — 5 752 clés — 1 - règle directe

- **Extensions** : pdf 1438, tif 1438, txt 1438, xml 1438
- **Rôles** : ZO 2876, ZD 1438, ZA 1438
- **Préfixe commun** : `PRA_CTG` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `PRA_CTG_N8_N3` — 5 752 (100%) — ex. `PRA_CTG_19000415_001.pdf`
- **Impact de l'indicateur** : 5 752 clés (100.0%) suivent la règle directe — ex. `PRA_CTG_19000415_001.pdf` → `PRA_CTG_19000415_ZD_001.pdf`
  - 5752 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 5752 objet(s) déjà sur S3 à renommer ; 2876 publié(s)

## PRA_ERT — 377 078 clés — 2 - règle directe + exceptions à lister

- **Extensions** : pdf 94387, txt 94387, tif 94386, xml 93918
- **Rôles** : ZO 188305, ZD 94387, ZA 94386
- **Préfixe commun** : `PRA_ERT` ; **numéro de vue** : N3 ; **2 motif(s)**
- **Règle de nommage observée** :
  - `PRA_ERT_N8_N3` — 377 076 (100%) — ex. `PRA_ERT_18950502_001.pdf`
  - `PRA_ERT_N8_# ` — 2 (0%) — ex. `PRA_ERT_19411015_004 .pdf`
- **Impact de l'indicateur** : 377 076 clés (100.0%) suivent la règle directe — ex. `PRA_ERT_18950502_001.pdf` → `PRA_ERT_18950502_ZD_001.pdf`
  - 2 nom(s) sans numéro final (autre 2 ; ex. `PRA_ERT_19411015_004 .pdf`)
  - 377078 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 2 nom(s) avec espace, parenthèse ou point
  - 377078 objet(s) déjà sur S3 à renommer ; 185403 publié(s)

## PRA_IND — 64 400 clés — 1 - règle directe

- **Extensions** : pdf 16358, tif 16358, xml 16307, txt 15377
- **Rôles** : ZO 31684, ZD 16358, ZA 16358
- **Préfixe commun** : `PRA_IND` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `PRA_IND_N8_N3` — 64 400 (100%) — ex. `PRA_IND_18400101_001.pdf`
- **Impact de l'indicateur** : 64 400 clés (100.0%) suivent la règle directe — ex. `PRA_IND_18400101_001.pdf` → `PRA_IND_18400101_ZD_001.pdf`
  - 64400 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 64400 objet(s) déjà sur S3 à renommer ; 32665 publié(s)

## PRA_JRX — 595 291 clés — 1 - règle directe

- **Extensions** : tif 149171, pdf 149132, xml 148814, txt 148174
- **Rôles** : ZO 296988, ZA 149171, ZD 149132
- **Préfixe commun** : `PRA_JRX` ; **numéro de vue** : N3 ; **4 motif(s)**
- **Règle de nommage observée** :
  - `PRA_JRX_N8_N3` — 587 572 (99%) — ex. `PRA_JRX_18560618_001.pdf`
  - `PRA_JRX_N8_<L>_N3` — 7 595 (1%) — ex. `PRA_JRX_18980903_M_001.pdf`
  - `PRA_JRX_N8_<SERIE>_N3` — 108 (0%) — ex. `PRA_JRX_19070102_ES_001.pdf`
  - … 1 autre(s) motif(s)  16 clés
- **Impact de l'indicateur** : 595 291 clés (100.0%) suivent la règle directe — ex. `PRA_JRX_18560618_001.pdf` → `PRA_JRX_18560618_ZD_001.pdf`
  - 7595 nom(s) avec une lettre avant la vue (M 6736, S 859 ; ex. `PRA_JRX_18980903_M_001.pdf` → `PRA_JRX_18980903_M_ZD_001.pdf`)
  - 595291 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 595291 objet(s) déjà sur S3 à renommer ; 284717 publié(s)

## PRA_RTG — 3 367 clés — 1 - règle directe

- **Extensions** : pdf 848, tif 848, txt 848, xml 823
- **Rôles** : ZO 1671, ZD 848, ZA 848
- **Préfixe commun** : `PRA_RTG` ; **numéro de vue** : N3 ; **1 motif(s)**
- **Règle de nommage observée** :
  - `PRA_RTG_N8_N3` — 3 367 (100%) — ex. `PRA_RTG_18910517_001.pdf`
- **Impact de l'indicateur** : 3 367 clés (100.0%) suivent la règle directe — ex. `PRA_RTG_18910517_001.pdf` → `PRA_RTG_18910517_ZD_001.pdf`
  - 3367 clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)
  - 3367 objet(s) déjà sur S3 à renommer ; 1671 publié(s)
