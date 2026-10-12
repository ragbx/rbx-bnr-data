# 2. Règles de nommage avec l'indicateur de rôle, par corpus

Source : `results/ref/_ref_files_20260630.csv.gz` — 2 205 685 s3_key, 70 corpus. Lecture seule.

Indicateur inséré juste avant le numéro de vue : `_ZA_` conservation, `_ZD_` diffusion, `_ZO_` OCR. Les fichiers METS gardent leur nom (suffixe `.mets.xml`). Les règles actuelles sont dans `1_nommage_actuel.md`.

## Règle générale (pseudo-code)

```
ROLE = { tif, tiff, wav            -> "ZA"   # conservation
         jpg, jpeg, pdf, png,
         mp3, mp4, mov, wmv        -> "ZD"   # diffusion
         txt, xml                  -> "ZO" } # OCR

fonction ajouter_indicateur(nom):
    stem, ext = couper_au_dernier_point(nom)
    si stem finit par ".mets":                 # METS : déjà identifié
        retourner nom
    role = ROLE[minuscules(ext)]               # l'extension d'origine est conservée
    si stem correspond à ^(.*)_(\d+)$ :       # le dernier segment est le numéro de vue
        retourner \1 + "_" + role + "_" + \2 + "." + ext
    sinon:
        signaler(nom)                          # pas de numéro de vue : cas à trancher

fonction lire_indicateur(nom):
    si nom correspond à ^(.*)_(Z[ADO])_(\d+)\.([^.]+)$ :
        retourner (doc=\1, role=\2, vue=\3, ext=\4)
    retourner aucun                            # ancien nom, METS ou cas particulier
```

## Lecture des fiches

- Une **structure** regroupe les noms de même forme (mêmes segments entre underscores, mêmes alternances chiffres/lettres).
- **gabarit** : chaque segment nommé s'écrit `<nom:contenu>` ; `N3` = nombre à 3 chiffres, `N1-3` = 1 à 3 chiffres, `{A|B}` = valeurs observées, `{sigle}` / `{lettre}` = trop de valeurs pour les lister. Le texte hors chevrons est fixe.
- **regex** : `doc` englobe tout ce qui précède la vue ; ses segments sont des groupes nommés imbriqués, suivis de `role` (l'indicateur), `vue` et `ext`.
- **Segments de `doc`**, nommés d'après leur forme et leurs valeurs (ce sont des déductions, pas une norme) :
  - `corpus_code` : le code du corpus (`PRA_JRX`, `MED_CP`), quand il figure tel quel dans le nom ;
  - `date` : 8 chiffres formant une date AAAAMMJJ ; `annee` : 4 chiffres entre 1500 et 2030 ; `mois` : 2 chiffres de 01 à 12 après une année ; `periode` : deux années reliées par un tiret ;
  - `cote` : cote d'archives, réservée aux corpus AMR : des chiffres, des lettres, puis éventuellement des chiffres (`4FI`, `1D008`, `499W001`, `6Fi022`). Une cote n'est pas un code de corpus : `AMR_4FI` est le corpus, `4FI` la cote ;
  - `niv_X` : une lettre ou un sigle fixe suivi d'un numéro (`D01`, `L1`, `PH02`) ; `sous_serie` : sigle variable suivi d'un numéro (`LFP1`, `CAR1`) ;
  - `serie` : sigle en lettres prenant plusieurs valeurs ; `lettre` : lettre seule ;
  - `num`, `num2`… : nombre sans signification déduite ; `seg` : segment d'une autre forme.
- **Identifiant du ref** : comparaison du nom avec la colonne `unitid` du ref (zéros de tête et casse ignorés), sur un échantillon de la structure. Elle indique jusqu'à quel segment va cet identifiant et ce qui vient après.
- Les regex décrivent les noms **observés** : largeurs de nombres et listes de sigles sont celles du ref.

- **Statuts** : *règle directe* = l'indicateur s'insère avant le dernier nombre ; *sous-vue probable* = même règle, mais le dernier nombre pourrait être une sous-vue ; *sans numéro de vue* et *fichier de document* = nom laissé tel quel en attendant une décision ; *METS* = nom inchangé.

## AMR_10FI — 164 clés, 2 structure(s)

164 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 164 nom(s) sur 164.

**162 clés (98.78 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N2}FI>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{2}FI)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_10FI_001_02.tif
après   : RBX_AMR_PHO_10FI_001_ZA_02.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`10Fi1/2`).

**2 clés (1.22 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N2}Fi>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{2}Fi)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_10Fi_003_14.tif
après   : RBX_AMR_PHO_10Fi_003_ZA_14.tif
```

## AMR_21J — 285 clés, 1 structure(s)

285 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 285 nom(s) sur 285.

**285 clés (100.0 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N2}J>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{2}J))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_21J_001.tif
après   : RBX_AMR_PHO_21J_ZA_001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`21J1`).

## AMR_2I — 15 703 clés, 1 structure(s)

15 703 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus se lit par le fonds et la cote dans 15 703 nom(s).

**15 703 clés (100.0 %) — règle directe**

```
gabarit : RBX_AMR_<cote:{N1}I>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_(?P<cote>\d{1}I)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>JPG|jpg)$
avant   : RBX_AMR_2I_072_001.JPG
après   : RBX_AMR_2I_072_ZD_001.JPG
```

## AMR_310W — 1 136 clés, 3 structure(s)

1 133 clés (99.7%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 1 136 nom(s) sur 1 136.

**1 078 clés (94.89 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_310W_001_01.tif
après   : RBX_AMR_PHO_310W_001_ZA_01.tif
```

**55 clés (4.84 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<num:N3>_<num2:N2>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<num>\d{3})_(?P<num2>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_310W_064_01_01.tif
après   : RBX_AMR_PHO_310W_064_01_ZA_01.tif
```

**3 clés (0.26 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<seg:{N3}-{N1}>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<seg>\d{3}\-\d{1}))\.(?P<ext>tif)$
exemple : RBX_AMR_PHO_310W_163-1.tif
```

## AMR_3D — 246 clés, 2 structure(s)

246 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus se lit par le fonds et la cote dans 246 nom(s).

**199 clés (80.89 %) — règle directe**

```
gabarit : RBX_AMR_<cote:{N1}D>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_(?P<cote>\d{1}D)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>JPG)$
avant   : RBX_AMR_3D_130_001.JPG
après   : RBX_AMR_3D_130_ZD_001.JPG
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`3D130/1`).

**47 clés (19.11 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_<cote:{N1}D>_<num:N3>_<num2:N1-3>_<role:ZA|ZD|ZO>_<vue:N1-2>
regex   : ^(?P<doc>RBX_AMR_(?P<cote>\d{1}D)_(?P<num>\d{3})_(?P<num2>\d{1,3}))_(?P<role>Z[ADO])_(?P<vue>\d{1,2})\.(?P<ext>JPG|tif)$
avant   : RBX_AMR_3D_195_1_01.tif
après   : RBX_AMR_3D_195_1_ZA_01.tif
```

## AMR_3FI — 4 843 clés, 2 structure(s)

4 843 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 4 843 nom(s) sur 4 843.

**4 822 clés (99.57 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N1}FI>_<role:ZA|ZD|ZO>_<vue:N5>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{1}FI))_(?P<role>Z[ADO])_(?P<vue>\d{5})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_3FI_00001.tif
après   : RBX_AMR_PHO_3FI_ZA_00001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`3Fi1`).

**21 clés (0.43 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_PHO_<cote:{N1}FI>_<num:N5>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{1}FI)_(?P<num>\d{5}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_3FI_03934_01.tif
après   : RBX_AMR_PHO_3FI_03934_ZA_01.tif
```

## AMR_440W — 15 clés, 2 structure(s)

15 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 15 nom(s) sur 15.

**12 clés (80.0 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<num:N3>_<num2:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<num>\d{3})_(?P<num2>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_440W_038_001_01.tif
après   : RBX_AMR_PHO_440W_038_001_ZA_01.tif
```

**3 clés (20.0 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_440W_047_009.tif
après   : RBX_AMR_PHO_440W_047_ZA_009.tif
```

## AMR_4FI — 326 clés, 1 structure(s)

326 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 326 nom(s) sur 326.

**326 clés (100.0 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N1}FI>_<role:ZA|ZD|ZO>_<vue:N5>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{1}FI))_(?P<role>Z[ADO])_(?P<vue>\d{5})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_4FI_00001.tif
après   : RBX_AMR_PHO_4FI_ZA_00001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`4Fi1`).

## AMR_500W — 1 447 clés, 5 structure(s)

941 clés (65.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 1 447 nom(s) sur 1 447.

**437 clés (30.2 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<seg:{N3}-{N2}>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<seg>\d{3}\-\d{2}))\.(?P<ext>tif)$
exemple : RBX_AMR_PHO_500W_041-01.tif
```

**423 clés (29.23 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_500W_001_01.tif
après   : RBX_AMR_PHO_500W_001_ZA_01.tif
```

**386 clés (26.68 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_500W_278.tif
après   : RBX_AMR_PHO_500W_ZA_278.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`500W278`).

**132 clés (9.12 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<num:N3>_<num2:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<num>\d{3})_(?P<num2>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_500W_003_001_01.tif
après   : RBX_AMR_PHO_500W_003_001_ZA_01.tif
```

**69 clés (4.77 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<num:N3>_<seg:{N1}-{N2}>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W)_(?P<num>\d{3})_(?P<seg>\d{1}\-\d{2}))\.(?P<ext>tif)$
exemple : RBX_AMR_PHO_500W_044_1-01.tif
```

## AMR_507W — 496 clés, 1 structure(s)

0 clés (0.0%) reçoivent l'indicateur par la règle directe. Le code du corpus se lit par le fonds et la cote dans 496 nom(s).

**496 clés (100.0 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_<cote:{N3}W>_<num:N3>_<num2:N3>_<lettre:{D|T}>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_(?P<cote>\d{3}W)_(?P<num>\d{3})_(?P<num2>\d{3})_(?P<lettre>[DT]))\.(?P<ext>tif)$
exemple : RBX_AMR_507W_001_002_T.tif
```

## AMR_512W — 857 clés, 1 structure(s)

857 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 857 nom(s) sur 857.

**857 clés (100.0 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N3}W>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{3}W))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_512W_001.tif
après   : RBX_AMR_PHO_512W_ZA_001.tif
```

## AMR_5FI — 63 clés, 1 structure(s)

63 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 63 nom(s) sur 63.

**63 clés (100.0 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N1}FI>_<role:ZA|ZD|ZO>_<vue:N5>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{1}FI))_(?P<role>Z[ADO])_(?P<vue>\d{5})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_5FI_00001.tif
après   : RBX_AMR_PHO_5FI_ZA_00001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`5Fi1`).

## AMR_5H — 7 072 clés, 5 structure(s)

7 067 clés (99.9%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 7 072 nom(s) sur 7 072.

**5 830 clés (82.44 %) — règle directe**

```
gabarit : RBX_AMR_VIC_<cote:{N1}H>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_VIC_(?P<cote>\d{1}H)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>JPG)$
avant   : RBX_AMR_VIC_5H_002_001.JPG
après   : RBX_AMR_VIC_5H_002_ZD_001.JPG
```

**909 clés (12.85 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_VIC_<cote:{N1}H>_<num:N3>_<num2:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_VIC_(?P<cote>\d{1}H)_(?P<num>\d{3})_(?P<num2>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>JPG)$
avant   : RBX_AMR_VIC_5H_004_045_01.JPG
après   : RBX_AMR_VIC_5H_004_045_ZD_01.JPG
```

**325 clés (4.6 %) — règle directe**

```
gabarit : RBX_AMR_VIC_<cote:{N1}H{N3}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_VIC_(?P<cote>\d{1}H\d{3})_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>JPG)$
avant   : RBX_AMR_VIC_5H074_010_01.JPG
après   : RBX_AMR_VIC_5H074_010_ZD_01.JPG
```

**5 clés (0.07 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_VIC_<cote:{N1}H>_<num:N3>_<num2:N3>_<niv_PJ:PJ{N2}>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_VIC_(?P<cote>\d{1}H)_(?P<num>\d{3})_(?P<num2>\d{3})_(?P<niv_PJ>PJ\d{2}))\.(?P<ext>JPG)$
exemple : RBX_AMR_VIC_5H_031_056_PJ01.JPG
```

**3 clés (0.04 %) — règle directe**

```
gabarit : RBX_AMR_VIC_<cote:{N1}H{N3}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_VIC_(?P<cote>\d{1}H\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>JPG)$
avant   : RBX_AMR_VIC_5H075_001.JPG
après   : RBX_AMR_VIC_5H075_ZD_001.JPG
```

## AMR_6H — 76 933 clés, 7 structure(s)

76 590 clés (99.6%) reçoivent l'indicateur par la règle directe. Le code du corpus se lit par le fonds et la cote dans 76 590 nom(s). Le code du corpus est absent de 343 nom(s) sur 76 933.

**76 434 clés (99.35 %) — règle directe**

```
gabarit : RBX_AMR_<cote:{N1}H>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_(?P<cote>\d{1}H)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>JPG|jpg)$
avant   : RBX_AMR_6H_105_001.JPG
après   : RBX_AMR_6H_105_ZD_001.JPG
```

**327 clés (0.43 %) — sans numéro de vue : à trancher**

```
gabarit : <seg:{N1} H {N3} fiche ({N1-3})>   (inchangé)
regex   : ^(?P<doc>(?P<seg>\d{1}\ H\ \d{3}\ fiche\ \(\d{1,3}\)))\.(?P<ext>JPG)$
exemple : 6 H 165 fiche (0).JPG
```

**156 clés (0.2 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_<cote:{N1}H>_<num:N3>_<num2:N2-3>_<role:ZA|ZD|ZO>_<vue:N2-3>
regex   : ^(?P<doc>RBX_AMR_(?P<cote>\d{1}H)_(?P<num>\d{3})_(?P<num2>\d{2,3}))_(?P<role>Z[ADO])_(?P<vue>\d{2,3})\.(?P<ext>JPG)$
avant   : RBX_AMR_6H_125_062_01.JPG
après   : RBX_AMR_6H_125_062_ZD_01.JPG
```

**7 clés (0.01 %) — sans numéro de vue : à trancher**

```
gabarit : <niv_RU:RU{N1}A{N4}>   (inchangé)
regex   : ^(?P<doc>(?P<niv_RU>RU\d{1}A\d{4}))\.(?P<ext>JPG)$
exemple : RU8A2456.JPG
```

**5 clés (0.01 %) — sans numéro de vue : à trancher**

```
gabarit : <seg:Dépot série {N1} H ({N1})>   (inchangé)
regex   : ^(?P<doc>(?P<seg>Dépot\ série\ \d{1}\ H\ \(\d{1}\)))\.(?P<ext>JPG)$
exemple : Dépot série 6 H (1).JPG
```

**2 clés (0.0 %) — sans numéro de vue : à trancher**

```
gabarit : <seg:{N1} H {N3} fiche ({N2-3} recto)>   (inchangé)
regex   : ^(?P<doc>(?P<seg>\d{1}\ H\ \d{3}\ fiche\ \(\d{2,3}\ recto\)))\.(?P<ext>JPG)$
exemple : 6 H 165 fiche (325 recto).JPG
```

**2 clés (0.0 %) — sans numéro de vue : à trancher**

```
gabarit : <seg:{N1} H {N3} fiche ({N2-3} verso)>   (inchangé)
regex   : ^(?P<doc>(?P<seg>\d{1}\ H\ \d{3}\ fiche\ \(\d{2,3}\ verso\)))\.(?P<ext>JPG)$
exemple : 6 H 165 fiche (325 verso).JPG
```

## AMR_7J — 4 clés, 1 structure(s)

0 clés (0.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 4 nom(s) sur 4.

**4 clés (100.0 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_PHO_<cote:{N1}J{N2}>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{1}J\d{2}))\.(?P<ext>tif)$
exemple : RBX_AMR_PHO_7J42.tif
```

## AMR_8J — 98 clés, 2 structure(s)

98 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 98 nom(s) sur 98.

**96 clés (97.96 %) — règle directe**

```
gabarit : RBX_AMR_PHO_<cote:{N1}J>_<num:N2>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{1}J)_(?P<num>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_8J_52_000.tif
après   : RBX_AMR_PHO_8J_52_ZA_000.tif
```

**2 clés (2.04 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_PHO_<cote:{N1}J>_<num:N2>_<num2:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PHO_(?P<cote>\d{1}J)_(?P<num>\d{2})_(?P<num2>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_8J_91_007_01.tif
après   : RBX_AMR_PHO_8J_91_007_ZA_01.tif
```

## AMR_9FI — 879 clés, 1 structure(s)

879 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 879 nom(s) sur 879.

**879 clés (100.0 %) — règle directe**

```
gabarit : RBX_AMR_DIA_<cote:{N1}FI>_<role:ZA|ZD|ZO>_<vue:N5>
regex   : ^(?P<doc>RBX_AMR_DIA_(?P<cote>\d{1}FI))_(?P<role>Z[ADO])_(?P<vue>\d{5})\.(?P<ext>tif)$
avant   : RBX_AMR_DIA_9FI_00001.tif
après   : RBX_AMR_DIA_9FI_ZA_00001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`9Fi1`).

## AMR_AFF — 1 098 clés, 2 structure(s)

1 098 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**1 081 clés (98.45 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_AFF>_<num:N3>_D_<num2:N4>_<role:ZA|ZD|ZO>_<vue:N1>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_AFF)_(?P<num>\d{3})_D_(?P<num2>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{1})\.(?P<ext>tif)$
avant   : RBX_AMR_AFF_001_D_0001_1.tif
après   : RBX_AMR_AFF_001_D_0001_ZA_1.tif
```

**17 clés (1.55 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_AFF>_<num:N3>_D_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_AFF)_(?P<num>\d{3})_D)_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif)$
avant   : RBX_AMR_AFF_001_D_0973.tif
après   : RBX_AMR_AFF_001_D_ZA_0973.tif
```

## AMR_AVI — 159 clés, 3 structure(s)

0 clés (0.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**148 clés (93.08 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_AVI>_<cote:{N4}VM{N4}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_AVI)_(?P<cote>\d{4}VM\d{4}))\.(?P<ext>mp4)$
exemple : RBX_AMR_AVI_0676VM0007.mp4
```

**6 clés (3.77 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_AVI>_<cote:{N4}FT{N4}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_AVI)_(?P<cote>\d{4}FT\d{4}))\.(?P<ext>mp4)$
exemple : RBX_AMR_AVI_0676FT0004.mp4
```

**5 clés (3.14 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_AVI>_<cote:{N4}FS{N4}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_AVI)_(?P<cote>\d{4}FS\d{4}))\.(?P<ext>mov|mp4)$
exemple : RBX_AMR_AVI_0676FS0001.mp4
```

## AMR_CAD — 296 clés, 4 structure(s)

295 clés (99.7%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 20 nom(s) sur 296.

**188 clés (63.51 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_CAD>_<annee:N4>_<cote:{N1}G{N4}>_<num:N1>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CAD)_(?P<annee>\d{4})_(?P<cote>\d{1}G\d{4})_(?P<num>\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg|tif)$
avant   : RBX_AMR_CAD_1847_1G1002_1_01.jpg
après   : RBX_AMR_CAD_1847_1G1002_1_ZD_01.jpg
```

**87 clés (29.39 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_CAD>_<annee:N4>_<cote:{N1}G{N4}>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CAD)_(?P<annee>\d{4})_(?P<cote>\d{1}G\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg|tif)$
avant   : RBX_AMR_CAD_1804_1G0007_01.jpg
après   : RBX_AMR_CAD_1804_1G0007_ZD_01.jpg
```

**20 clés (6.76 %) — règle directe**

```
gabarit : RBX_AMR_PLA_<annee:N4>_<cote:{N1}G{N4}>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PLA_(?P<annee>\d{4})_(?P<cote>\d{1}G\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg)$
avant   : RBX_AMR_PLA_1819_1G1001_01.jpg
après   : RBX_AMR_PLA_1819_1G1001_ZD_01.jpg
```

**1 clés (0.34 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_CAD>_<annee:N4>_<cote:{N1}G{N4}>_TA   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CAD)_(?P<annee>\d{4})_(?P<cote>\d{1}G\d{4})_TA)\.(?P<ext>jpg)$
exemple : RBX_AMR_CAD_1884_1G1028_TA.jpg
```

## AMR_CIM — 16 070 clés, 8 structure(s)

16 060 clés (99.9%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**9 178 clés (57.11 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_CIM>_CPE_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CIM)_CPE_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_CIM_CPE_18500329_000.tif
après   : RBX_AMR_CIM_CPE_18500329_ZA_000.tif
```

**6 784 clés (42.22 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_CIM>_CPE_<date:N8>_<lettre:{lettre}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CIM)_CPE_(?P<date>\d{8})_(?P<lettre>[A-Z]))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>TIF|tif)$
avant   : RBX_AMR_CIM_CPE_18530804_A_000.tif
après   : RBX_AMR_CIM_CPE_18530804_A_ZA_000.tif
```

**95 clés (0.59 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_CIM>_RCP_<lettre:{lettre}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CIM)_RCP_(?P<lettre>[A-Z]))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_CIM_RCP_A_001.tif
après   : RBX_AMR_CIM_RCP_A_ZA_001.tif
```

**4 clés (0.02 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_CIM>_CPE_<date:N8>_<lettre:{B|C|F|H}>_<seg:{N3}a>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CIM)_CPE_(?P<date>\d{8})_(?P<lettre>[BCFH])_(?P<seg>\d{3}a))\.(?P<ext>tif)$
exemple : RBX_AMR_CIM_CPE_18680725_C_007a.tif
```

**3 clés (0.02 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_CIM>_RCP_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CIM)_RCP)_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_CIM_RCP_001.tif
après   : RBX_AMR_CIM_RCP_ZA_001.tif
```

**3 clés (0.02 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_CIM>_CPE_<date:N8>_<seg:{N3}a>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CIM)_CPE_(?P<date>\d{8})_(?P<seg>\d{3}a))\.(?P<ext>tif)$
exemple : RBX_AMR_CIM_CPE_18650819_005a.tif
```

**2 clés (0.01 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_CIM>_CPE_<date:N8>_<lettre:{B|H}>_<seg:{N3}b>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CIM)_CPE_(?P<date>\d{8})_(?P<lettre>[BH])_(?P<seg>\d{3}b))\.(?P<ext>tif)$
exemple : RBX_AMR_CIM_CPE_18720911_H_008b.tif
```

**1 clés (0.01 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_CIM>_CPE_<date:N8>_H_<seg:{N3}c>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_CIM)_CPE_(?P<date>\d{8})_H_(?P<seg>\d{3}c))\.(?P<ext>tif)$
exemple : RBX_AMR_CIM_CPE_18720911_H_008c.tif
```

## AMR_DEL — 169 733 clés, 6 structure(s)

169 453 clés (99.8%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**109 060 clés (64.25 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_DEL>_<cote:{N1}D{N3}>_<annee:N4>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_DEL)_(?P<cote>\d{1}D\d{3})_(?P<annee>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif|xml)$
avant   : RBX_AMR_DEL_1D008_0001_0001.tif
après   : RBX_AMR_DEL_1D008_0001_ZA_0001.tif
```
À noter : 2 fichier(s) de document dont le dernier nombre n'est pas une vue.

**47 352 clés (27.9 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_DEL>_<cote:{N3}W{N3}>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_DEL)_(?P<cote>\d{3}W\d{3})_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>jpg|xml)$
avant   : RBX_AMR_DEL_499W001_19830221_0001.jpg
après   : RBX_AMR_DEL_499W001_19830221_ZD_0001.jpg
```

**11 937 clés (7.03 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_DEL>_<cote:{N1}D{N3}>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_DEL)_(?P<cote>\d{1}D\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif)$
avant   : RBX_AMR_DEL_1D001_0001.tif
après   : RBX_AMR_DEL_1D001_ZA_0001.tif
```

**1 102 clés (0.65 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_DEL>_<cote:{N1}D{N3}>_<periode:{N4}-{N4}>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_DEL)_(?P<cote>\d{1}D\d{3})_(?P<periode>\d{4}\-\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif|xml)$
avant   : RBX_AMR_DEL_1D170_1915-1918_0001.tif
après   : RBX_AMR_DEL_1D170_1915-1918_ZA_0001.tif
```

**280 clés (0.16 %) — fichier de document, sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_DEL>_<cote:{N3}W{N3}>_<date:N8>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_DEL)_(?P<cote>\d{3}W\d{3})_(?P<date>\d{8}))\.(?P<ext>pdf|txt)$
exemple : RBX_AMR_DEL_499W001_19830221.pdf
```
À noter : 280 fichier(s) de document dont le dernier nombre n'est pas une vue.

**2 clés (0.0 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_<corpus_code:AMR_DEL>_<cote:{N1}D{N3}>_<annee:N4>_<num:N4>_<role:ZA|ZD|ZO>_<vue:N1>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_DEL)_(?P<cote>\d{1}D\d{3})_(?P<annee>\d{4})_(?P<num>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{1})\.(?P<ext>tif|xml)$
avant   : RBX_AMR_DEL_1D225_1972_0022_1.tif
après   : RBX_AMR_DEL_1D225_1972_0022_ZA_1.tif
```
À noter : 1 sous-vue(s) avérée(s).

## AMR_EC — 260 628 clés, 123 structure(s)

260 432 clés (99.9%) reçoivent l'indicateur par la règle directe. 1479 nouveau(x) nom(s) resteraient accepté(s) par une règle actuelle du corpus. Le code du corpus est absent de 260 628 nom(s) sur 260 628.

**107 728 clés (41.33 %) — règle directe**

```
gabarit : <serie:{MI|Reg}>_EC_<num:N2-3>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>(?P<serie>(?:MI|Reg))_EC_(?P<num>\d{2,3}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>JPG|jpg|txt)$
avant   : MI_EC_01_0001.jpg
après   : MI_EC_01_ZD_0001.jpg
```

**44 380 clés (17.03 %) — règle directe**

```
gabarit : <niv_D:D{N4}>_<num:N2>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<niv_D>D\d{4})_(?P<num>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : D1908_01_001.jpg
après   : D1908_01_ZD_001.jpg
```

**26 654 clés (10.23 %) — règle directe**

```
gabarit : <niv_N:N{N4}>_<num:N2>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<niv_N>N\d{4})_(?P<num>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : N1913_03_001.jpg
après   : N1913_03_ZD_001.jpg
```

**23 882 clés (9.16 %) — règle directe**

```
gabarit : <niv_M:M{N4}>_<num:N2>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<niv_M>M\d{4})_(?P<num>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : M1919_08_001.jpg
après   : M1919_08_ZD_001.jpg
```

**21 571 clés (8.28 %) — règle directe**

```
gabarit : MI_EC_<seg:{N2} >_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>MI_EC_(?P<seg>\d{2}\ ))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>jpg)$
avant   : MI_EC_28 _0001.jpg
après   : MI_EC_28 _ZD_0001.jpg
```

**12 489 clés (4.79 %) — règle directe**

```
gabarit : <niv_FRAD:FRAD{N3}>_<cote:{N1}ETD>_<num:N3>_<num2:N2>_<num3:N1>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>(?P<niv_FRAD>FRAD\d{3})_(?P<cote>\d{1}ETD)_(?P<num>\d{3})_(?P<num2>\d{2})_(?P<num3>\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>jpg)$
avant   : FRAD059_3ETD_512_01_1_0001.jpg
après   : FRAD059_3ETD_512_01_1_ZD_0001.jpg
```

**4 785 clés (1.84 %) — règle directe**

```
gabarit : <niv_D:D{N4}>_<lettre:{B|T}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<niv_D>D\d{4})_(?P<lettre>[BT]))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : D1908_T_001.jpg
après   : D1908_T_ZD_001.jpg
```

**4 564 clés (1.75 %) — règle directe**

```
gabarit : <niv_TD:TD{N4}>_<annee:N4>_<serie:{sigle}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<niv_TD>TD\d{4})_(?P<annee>\d{4})_(?P<serie>[A-Za-z]+))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : TD1933_1942_Z_001.jpg
après   : TD1933_1942_Z_ZD_001.jpg
```

**3 147 clés (1.21 %) — règle directe**

```
gabarit : <niv_D:D{N4}>_<num:N2>_<lettre:{A|B}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<niv_D>D\d{4})_(?P<num>\d{2})_(?P<lettre>[AB]))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : D1995_01_A_001.jpg
après   : D1995_01_A_ZD_001.jpg
```

**2 621 clés (1.01 %) — règle directe**

```
gabarit : <niv_M:M{N4}>_T_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<niv_M>M\d{4})_T)_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : M1908_T_001.jpg
après   : M1908_T_ZD_001.jpg
```

… 113 autre(s) structure(s), 8 807 clés (détail dans le CSV).

## AMR_GUE — 1 022 clés, 3 structure(s)

1 022 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 1 022 nom(s) sur 1 022.

**632 clés (61.84 %) — règle directe**

```
gabarit : RBX_AMR_AFF_<cote:{N1}H>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_AMR_AFF_(?P<cote>\d{1}H))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>jpg|tif|xml)$
avant   : RBX_AMR_AFF_7H_0001.jpg
après   : RBX_AMR_AFF_7H_ZD_0001.jpg
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`GUE_AFF_7H0001_D01`).

**234 clés (22.9 %) — règle directe**

```
gabarit : RBX_AMR_AFF_<cote:{N1}H>_<num:N4>_<num2:N2>_<role:ZA|ZD|ZO>_<vue:N1-2>
regex   : ^(?P<doc>RBX_AMR_AFF_(?P<cote>\d{1}H)_(?P<num>\d{4})_(?P<num2>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{1,2})\.(?P<ext>jpg|tif|xml)$
avant   : RBX_AMR_AFF_7H_0061_02_01.jpg
après   : RBX_AMR_AFF_7H_0061_02_ZD_01.jpg
```

**156 clés (15.26 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_AFF_<cote:{N1}H>_<num:N4>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_AFF_(?P<cote>\d{1}H)_(?P<num>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg|tif|xml)$
avant   : RBX_AMR_AFF_7H_0011_01.jpg
après   : RBX_AMR_AFF_7H_0011_ZD_01.jpg
```

## AMR_OBJ — 100 clés, 1 structure(s)

100 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**100 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_OBJ|AMR_Obj>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_OBJ|AMR_Obj)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_OBJ_042_01.tif
après   : RBX_AMR_OBJ_042_ZA_01.tif
```

## AMR_PAR — 245 clés, 6 structure(s)

221 clés (90.2%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**206 clés (84.08 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PAR>_LAB_<niv_PH:PH{N2}>_<num:N2-3>_<role:ZA|ZD|ZO>_<vue:N2-3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PAR)_LAB_(?P<niv_PH>PH\d{2})_(?P<num>\d{2,3}))_(?P<role>Z[ADO])_(?P<vue>\d{2,3})\.(?P<ext>jpg|tif)$
avant   : RBX_AMR_PAR_LAB_PH02_005_02.tif
après   : RBX_AMR_PAR_LAB_PH02_005_ZA_02.tif
```
À noter : 15 sous-vue(s) avérée(s).

**22 clés (8.98 %) — fichier de document, sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_PAR>_LAB_<niv_PH:PH{N2}>_<num:N3-4>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PAR)_LAB_(?P<niv_PH>PH\d{2})_(?P<num>\d{3,4}))\.(?P<ext>jpg|tif)$
exemple : RBX_AMR_PAR_LAB_PH02_050.jpg
```
À noter : 14 fichier(s) de document dont le dernier nombre n'est pas une vue.

**14 clés (5.71 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PAR>_LES_<niv_D:D{N2}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PAR)_LES_(?P<niv_D>D\d{2})_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_PAR_LES_D01_006_027.tif
après   : RBX_AMR_PAR_LES_D01_006_ZA_027.tif
```

**1 clés (0.41 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_PAR>_LAB_<niv_PH:PH{N2}>_<seg:{N3} ({N1})>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PAR)_LAB_(?P<niv_PH>PH\d{2})_(?P<seg>\d{3}\ \(\d{1}\)))\.(?P<ext>tif)$
exemple : RBX_AMR_PAR_LAB_PH05_113 (2).tif
```

**1 clés (0.41 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_PAR>_LAB_<niv_PH:PH{N2}>_<num:N3>_<seg:{N2} ({N1})>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PAR)_LAB_(?P<niv_PH>PH\d{2})_(?P<num>\d{3})_(?P<seg>\d{2}\ \(\d{1}\)))\.(?P<ext>tif)$
exemple : RBX_AMR_PAR_LAB_PH32_090_01 (2).tif
```

**1 clés (0.41 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PAR>_LAB_OBJ_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PAR)_LAB_OBJ_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>JPG)$
avant   : RBX_AMR_PAR_LAB_OBJ_012_02.JPG
après   : RBX_AMR_PAR_LAB_OBJ_012_ZD_02.JPG
```

## AMR_PHO — 1 266 clés, 1 structure(s)

1 266 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**1 266 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PHO>_COM_<niv_AMR:AMR{N1}>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PHO)_COM_(?P<niv_AMR>AMR\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif)$
avant   : RBX_AMR_PHO_COM_AMR1_0001.tif
après   : RBX_AMR_PHO_COM_AMR1_ZA_0001.tif
```

## AMR_PLA — 244 clés, 6 structure(s)

163 clés (66.8%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 1 nom(s) sur 244.

**138 clés (56.56 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PLA>_<cote:{N1}Fi{N3}>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PLA)_(?P<cote>\d{1}Fi\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg|tif)$
avant   : RBX_AMR_PLA_6Fi022_01.tif
après   : RBX_AMR_PLA_6Fi022_ZA_01.tif
```

**79 clés (32.38 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_PLA>_<cote:{N1}Fi{N3}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PLA)_(?P<cote>\d{1}Fi\d{3}))\.(?P<ext>jpg|tif)$
exemple : RBX_AMR_PLA_6Fi005.tif
```

**20 clés (8.2 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PLA>_<cote:{N1}G{N4}>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PLA)_(?P<cote>\d{1}G\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_AMR_PLA_1G1001_10.tif
après   : RBX_AMR_PLA_1G1001_ZA_10.tif
```

**4 clés (1.64 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_<corpus_code:AMR_PLA>_<cote:{N1}Fi{N3}>_<num:N2>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PLA)_(?P<cote>\d{1}Fi\d{3})_(?P<num>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg|tif)$
avant   : RBX_AMR_PLA_6Fi038_01_01.jpg
après   : RBX_AMR_PLA_6Fi038_01_ZD_01.jpg
```

**2 clés (0.82 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_PLA>_<cote:{N1}FI{N3}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PLA)_(?P<cote>\d{1}FI\d{3}))\.(?P<ext>jpg|tif)$
exemple : RBX_AMR_PLA_6FI004.jpg
```

**1 clés (0.41 %) — règle directe**

```
gabarit : RBX_AMR_ANC_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_ANC)_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : RBX_AMR_ANC_005.jpg
après   : RBX_AMR_ANC_ZD_005.jpg
```

## AMR_POP — 93 576 clés, 2 structure(s)

93 576 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**93 575 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_POP>_<annee:N4>_<num:N2-3>_<role:ZA|ZD|ZO>_<vue:N3-4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_POP)_(?P<annee>\d{4})_(?P<num>\d{2,3}))_(?P<role>Z[ADO])_(?P<vue>\d{3,4})\.(?P<ext>JPG|TIFF|jpg|tif|txt)$
avant   : RBX_AMR_POP_1799_01_001.jpg
après   : RBX_AMR_POP_1799_01_ZD_001.jpg
```

**1 clés (0.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_POP>_<annee:N4>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_POP)_(?P<annee>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf)$
avant   : RBX_AMR_POP_1851_000.pdf
après   : RBX_AMR_POP_1851_ZD_000.pdf
```

## AMR_PR — 4 199 clés, 8 structure(s)

4 197 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 102 nom(s) sur 4 199.

**3 624 clés (86.31 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PR>_<serie:{COR|FLO|HFR|REQ}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N1-4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PR)_(?P<serie>(?:COR|FLO|HFR|REQ))_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{1,4})\.(?P<ext>jpg|tif)$
avant   : RBX_AMR_PR_COR_001_0001.tif
après   : RBX_AMR_PR_COR_001_ZA_0001.tif
```

**380 clés (9.05 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PR>_<serie:{CAN|COR|FLO|HDV|HFR|REQ}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PR)_(?P<serie>(?:CAN|COR|FLO|HDV|HFR|REQ)))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif|xml)$
avant   : RBX_AMR_PR_CAN_001.tif
après   : RBX_AMR_PR_CAN_ZA_001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`AMR_PR_CAN_001`) ; 6 fichier(s) de document dont le dernier nombre n'est pas une vue.

**91 clés (2.17 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PR>_FLO_<num:N3>_<cote:{N1}D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PR)_FLO_(?P<num>\d{3})_(?P<cote>\d{1}D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_PR_FLO_103_3D31_001.tif
après   : RBX_AMR_PR_FLO_103_3D31_ZA_001.tif
```

**56 clés (1.33 %) — règle directe**

```
gabarit : RBX_AMR_<serie:{BAR|PDS}>_<cote:{N1}M{N3}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2-3>
regex   : ^(?P<doc>RBX_AMR_(?P<serie>(?:BAR|PDS))_(?P<cote>\d{1}M\d{3})_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2,3})\.(?P<ext>jpg|tif)$
avant   : RBX_AMR_BAR_5M144_003_01.jpg
après   : RBX_AMR_BAR_5M144_003_ZD_01.jpg
```

**44 clés (1.05 %) — règle directe**

```
gabarit : RBX_AMR_<serie:{BAR|GDL|PDS}>_<cote:{N1}M{N3}>_<niv_nc:nc{N2}>_<role:ZA|ZD|ZO>_<vue:N2-3>
regex   : ^(?P<doc>RBX_AMR_(?P<serie>(?:BAR|GDL|PDS))_(?P<cote>\d{1}M\d{3})_(?P<niv_nc>nc\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2,3})\.(?P<ext>jpg)$
avant   : RBX_AMR_BAR_5M143_nc01_01.jpg
après   : RBX_AMR_BAR_5M143_nc01_ZD_01.jpg
```

**2 clés (0.05 %) — règle directe**

```
gabarit : RBX_AMR_<serie:{CAN|PLA}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_(?P<serie>(?:CAN|PLA)))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : RBX_AMR_CAN_026.jpg
après   : RBX_AMR_CAN_ZD_026.jpg
```

**1 clés (0.02 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_PR>_CAN_<seg:{N3}a>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PR)_CAN_(?P<seg>\d{3}a))\.(?P<ext>tif)$
exemple : RBX_AMR_PR_CAN_014a.tif
```

**1 clés (0.02 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_PR>_CAN_<seg:{N3}b>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PR)_CAN_(?P<seg>\d{3}b))\.(?P<ext>tif)$
exemple : RBX_AMR_PR_CAN_014b.tif
```

## AMR_PUV — 9 733 clés, 6 structure(s)

9 453 clés (97.1%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 9 733 nom(s) sur 9 733.

**9 387 clés (96.45 %) — règle directe**

```
gabarit : RBX_AMR_<niv_PUV:PUV{N1}>_<annee:N4>_<num:N2>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_(?P<niv_PUV>PUV\d{1})_(?P<annee>\d{4})_(?P<num>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif|xml)$
avant   : RBX_AMR_PUV1_1965_02_001.jpg
après   : RBX_AMR_PUV1_1965_02_ZD_001.jpg
```

**264 clés (2.71 %) — fichier de document, sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_<niv_PUV:PUV{N1}>_<annee:N4>_<num:N2>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_(?P<niv_PUV>PUV\d{1})_(?P<annee>\d{4})_(?P<num>\d{2}))\.(?P<ext>pdf)$
exemple : RBX_AMR_PUV1_2014_10.pdf
```
À noter : 180 fichier(s) de document dont le dernier nombre n'est pas une vue.

**66 clés (0.68 %) — règle directe**

```
gabarit : RBX_AMR_<niv_PUV:PUV{N1}>_<annee:N4>_<cote:{N2}B>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_(?P<niv_PUV>PUV\d{1})_(?P<annee>\d{4})_(?P<cote>\d{2}B))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif|xml)$
avant   : RBX_AMR_PUV1_1982_12B_001.jpg
après   : RBX_AMR_PUV1_1982_12B_ZD_001.jpg
```

**13 clés (0.13 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_<niv_PUV:PUV{N1}>_<annee:N4>_<seg:{N2}-{N2}>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_(?P<niv_PUV>PUV\d{1})_(?P<annee>\d{4})_(?P<seg>\d{2}\-\d{2}))\.(?P<ext>pdf)$
exemple : RBX_AMR_PUV1_2015_06-08.pdf
```

**2 clés (0.02 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_<niv_PUV:PUV{N1}>_<annee:N4>_<cote:{N2}B>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_(?P<niv_PUV>PUV\d{1})_(?P<annee>\d{4})_(?P<cote>\d{2}B))\.(?P<ext>pdf)$
exemple : RBX_AMR_PUV1_1982_12B.pdf
```

**1 clés (0.01 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_<niv_PUV:PUV{N1}>_<annee:N4>_<seg:{N2}-{N2}B>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_(?P<niv_PUV>PUV\d{1})_(?P<annee>\d{4})_(?P<seg>\d{2}\-\d{2}B))\.(?P<ext>pdf)$
exemple : RBX_AMR_PUV1_2020_07-08B.pdf
```

## AMR_PVC — 19 170 clés, 2 structure(s)

18 888 clés (98.5%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**18 888 clés (98.53 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_PVC>_<cote:{N3}W{N3}>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PVC)_(?P<cote>\d{3}W\d{3})_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>jpg|xml)$
avant   : RBX_AMR_PVC_246W042_19830221_0001.jpg
après   : RBX_AMR_PVC_246W042_19830221_ZD_0001.jpg
```

**282 clés (1.47 %) — fichier de document, sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:AMR_PVC>_<cote:{N3}W{N3}>_<date:N8>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_PVC)_(?P<cote>\d{3}W\d{3})_(?P<date>\d{8}))\.(?P<ext>pdf|txt)$
exemple : RBX_AMR_PVC_246W042_19830221.pdf
```
À noter : 282 fichier(s) de document dont le dernier nombre n'est pas une vue.

## AMR_RAM — 36 843 clés, 4 structure(s)

36 828 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 26 nom(s) sur 36 843.

**35 175 clés (95.47 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_RAM>_<cote:{N1}D{N3}>_<annee:N4>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_RAM)_(?P<cote>\d{1}D\d{3})_(?P<annee>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif|xml)$
avant   : RBX_AMR_RAM_3D001_1862_0001.tif
après   : RBX_AMR_RAM_3D001_1862_ZA_0001.tif
```

**1 642 clés (4.46 %) — règle directe**

```
gabarit : RBX_<corpus_code:AMR_RAM>_<cote:{N1}D{N3}>_<periode:{N4}-{N4}>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>AMR_RAM)_(?P<cote>\d{1}D\d{3})_(?P<periode>\d{4}\-\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif|xml)$
avant   : RBX_AMR_RAM_3D020_1914-1930_0001.tif
après   : RBX_AMR_RAM_3D020_1914-1930_ZA_0001.tif
```

**15 clés (0.04 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_AMR_<cote:{N1}D>_<num:N3>_<periode:{N4}-{N4}>   (inchangé)
regex   : ^(?P<doc>RBX_AMR_(?P<cote>\d{1}D)_(?P<num>\d{3})_(?P<periode>\d{4}\-\d{4}))\.(?P<ext>pdf)$
exemple : RBX_AMR_3D_001_1862-1870.pdf
```

**11 clés (0.03 %) — règle directe**

```
gabarit : RBX_AMR_<cote:{N1}D>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_AMR_(?P<cote>\d{1}D)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>pdf)$
avant   : RBX_AMR_3D_014_1908.pdf
après   : RBX_AMR_3D_014_ZD_1908.pdf
```

## ARA_CPS — 174 clés, 2 structure(s)

174 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**146 clés (83.91 %) — règle directe**

```
gabarit : RBX_<corpus_code:ARA_CPS>_<niv_VI:VI{N1}>_<serie:{HOR|LEO|MAI|PAT|PIL|SAI|TRO|VER}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>ARA_CPS)_(?P<niv_VI>VI\d{1})_(?P<serie>(?:HOR|LEO|MAI|PAT|PIL|SAI|TRO|VER)))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|wav)$
avant   : RBX_ARA_CPS_VI2_HOR_001.jpg
après   : RBX_ARA_CPS_VI2_HOR_ZD_001.jpg
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`ARA_CPS_VI2_HOR_001`) ; 10 fichier(s) de document dont le dernier nombre n'est pas une vue.

**28 clés (16.09 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_<corpus_code:ARA_CPS>_<niv_VI:VI{N1}>_<serie:{PAT|SAI}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>ARA_CPS)_(?P<niv_VI>VI\d{1})_(?P<serie>(?:PAT|SAI))_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|wav)$
avant   : RBX_ARA_CPS_VI2_PAT_001_001.jpg
après   : RBX_ARA_CPS_VI2_PAT_001_ZD_001.jpg
```

## CSV_PAL — 242 clés, 1 structure(s)

242 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**242 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:CSV_PAL>_<annee:N4>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>CSV_PAL)_(?P<annee>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_CSV_PAL_1858_01.tif
après   : RBX_CSV_PAL_1858_ZA_01.tif
```

## LAI — 1 661 clés, 3 structure(s)

1 661 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 516 nom(s) sur 1 661.

**1 145 clés (68.93 %) — règle directe**

```
gabarit : RBX_<corpus_code:LAI>_<niv_D:D{N2}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>LAI)_(?P<niv_D>D\d{2})_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_LAI_D01_001_001.tif
après   : RBX_LAI_D01_001_ZA_001.tif
```

**416 clés (25.05 %) — règle directe**

```
gabarit : RBX_MED_LAI_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_MED_LAI_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_LAI_D04_002.tif
après   : RBX_MED_LAI_D04_ZA_002.tif
```

**100 clés (6.02 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_MED_LAI_<niv_D:D{N2}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_MED_LAI_(?P<niv_D>D\d{2})_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_LAI_D04_002_001.tif
après   : RBX_MED_LAI_D04_002_ZA_001.tif
```

## LAR_PUB — 3 731 clés, 1 structure(s)

3 731 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**3 731 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:LAR_PUB>_<serie:{AMM|CDR|VPR}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>LAR_PUB)_(?P<serie>(?:AMM|CDR|VPR))_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_LAR_PUB_AMM_001_000.tif
après   : RBX_LAR_PUB_AMM_001_ZA_000.tif
```

## MDF_MTX — 18 clés, 1 structure(s)

0 clés (0.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**18 clés (100.0 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MDF_MTX>_<annee:N4>_<niv_D:D{N2}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MDF_MTX)_(?P<annee>\d{4})_(?P<niv_D>D\d{2}))\.(?P<ext>mov)$
exemple : RBX_MDF_MTX_2012_D02.mov
```

## MED_AFF — 4 222 clés, 4 structure(s)

4 221 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**4 047 clés (95.86 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_AFF>_<num:N3>_<serie:{BIS|LOC|TER}>_<lettre:{A|B|C}>_<num2:N4>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_AFF)_(?P<num>\d{3})_(?P<serie>(?:BIS|LOC|TER))_(?P<lettre>[ABC])_(?P<num2>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_AFF_001_LOC_A_0065_001.tif
après   : RBX_MED_AFF_001_LOC_A_0065_ZA_001.tif
```

**99 clés (2.34 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_AFF>_<num:N3>_<lettre:{B|C}>_<num2:N1>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_AFF)_(?P<num>\d{3})_(?P<lettre>[BC])_(?P<num2>\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_AFF_001_B_1_001.tif
après   : RBX_MED_AFF_001_B_1_ZA_001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`AFF_001_B_1_001`).

**75 clés (1.78 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_AFF>_<num:N3>_A_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_AFF)_(?P<num>\d{3})_A)_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_AFF_001_A_001.tif
après   : RBX_MED_AFF_001_A_ZA_001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`AFF_001_A_001`).

**1 clés (0.02 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_AFF>_<num:N3>_A_<seg:{N3}bis>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_AFF)_(?P<num>\d{3})_A_(?P<seg>\d{3}bis))\.(?P<ext>tif)$
exemple : RBX_MED_AFF_001_A_043bis.tif
```

## MED_AVI — 31 clés, 1 structure(s)

0 clés (0.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**31 clés (100.0 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_AVI>_<seg:{N4}VM{N4}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_AVI)_(?P<seg>\d{4}VM\d{4}))\.(?P<ext>mp4)$
exemple : RBX_MED_AVI_0932VM0001.mp4
```

## MED_CHA — 363 clés, 2 structure(s)

363 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**295 clés (81.27 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_CHA>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_CHA))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif)$
avant   : RBX_MED_CHA_0001.tif
après   : RBX_MED_CHA_ZA_0001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`CHA_0001`) ; 2 fichier(s) de document dont le dernier nombre n'est pas une vue.

**68 clés (18.73 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_<corpus_code:MED_CHA>_<num:N4>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_CHA)_(?P<num>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_CHA_0011_001.tif
après   : RBX_MED_CHA_0011_ZA_001.tif
```
À noter : 4 sous-vue(s) avérée(s).

## MED_CP — 5 318 clés, 6 structure(s)

5 283 clés (99.3%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**4 830 clés (90.82 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_CP>_<niv_A:A{N2}>_<niv_L:L{N1-2}>_<niv_S:S{N1-2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_CP)_(?P<niv_A>A\d{2})_(?P<niv_L>L\d{1,2})_(?P<niv_S>S\d{1,2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpeg|jpg|tif)$
avant   : RBX_MED_CP_A01_L1_S1_001.tif
après   : RBX_MED_CP_A01_L1_S1_ZA_001.tif
```
À noter : 1 fichier(s) de document dont le dernier nombre n'est pas une vue.

**284 clés (5.34 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_<corpus_code:MED_CP>_<niv_A:A{N2}>_<niv_L:L{N1-2}>_<niv_S:S{N1-2}>_<num:N1-3>_<role:ZA|ZD|ZO>_<vue:N1-3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_CP)_(?P<niv_A>A\d{2})_(?P<niv_L>L\d{1,2})_(?P<niv_S>S\d{1,2})_(?P<num>\d{1,3}))_(?P<role>Z[ADO])_(?P<vue>\d{1,3})\.(?P<ext>jpg|tif)$
avant   : RBX_MED_CP_A03_L1_S1_1_001.tif
après   : RBX_MED_CP_A03_L1_S1_1_ZA_001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`CP_A03_L1_S1_1_001`) ; 1 sous-vue(s) avérée(s).

**164 clés (3.08 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_CP>_<niv_A:A{N2}>_<niv_L:L{N1}>_<niv_S:S{N1}>_<lettre:{A|B|a}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_CP)_(?P<niv_A>A\d{2})_(?P<niv_L>L\d{1})_(?P<niv_S>S\d{1})_(?P<lettre>[ABa]))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_MED_CP_A01_L1_S3_A_007.tif
après   : RBX_MED_CP_A01_L1_S3_A_ZA_007.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`CP_A01_L1_S3_A_005`).

**28 clés (0.53 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_CP>_<niv_A:A{N2}>_<niv_L:L{N1}>_<niv_S:S{N1}>_<lettre:{A|B}>_<seg:{N3}.tif>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_CP)_(?P<niv_A>A\d{2})_(?P<niv_L>L\d{1})_(?P<niv_S>S\d{1})_(?P<lettre>[AB])_(?P<seg>\d{3}\.tif))\.(?P<ext>tif)$
exemple : RBX_MED_CP_A13_L2_S1_A_012.tif.tif
```

**7 clés (0.13 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_CP>_<niv_A:A{N2}>_<niv_L:L{N1}>_<niv_S:S{N1}>_<seg:{N3}.tif>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_CP)_(?P<niv_A>A\d{2})_(?P<niv_L>L\d{1})_(?P<niv_S>S\d{1})_(?P<seg>\d{3}\.tif))\.(?P<ext>tif)$
exemple : RBX_MED_CP_A13_L1_S1_014.tif.tif
```

**5 clés (0.09 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_<corpus_code:MED_CP>_<niv_A:A{N2}>_<niv_L:L{N1}>_<niv_S:S{N1}>_<lettre:{A|a}>_<num:N1-3>_<role:ZA|ZD|ZO>_<vue:N1-3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_CP)_(?P<niv_A>A\d{2})_(?P<niv_L>L\d{1})_(?P<niv_S>S\d{1})_(?P<lettre>[Aa])_(?P<num>\d{1,3}))_(?P<role>Z[ADO])_(?P<vue>\d{1,3})\.(?P<ext>tif)$
avant   : RBX_MED_CP_A06_L1_S1_A_010_1.tif
après   : RBX_MED_CP_A06_L1_S1_A_010_ZA_1.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`CP_A06_L1_S1_A_010`).

## MED_EPH — 846 clés, 1 structure(s)

846 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**846 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_EPH>_<sous_serie:{C|D|F|I|M}{N1}>_<niv_L:L{N1}>_<num:N3>_<num2:N2>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_EPH)_(?P<sous_serie>[CDFIM]\d{1})_(?P<niv_L>L\d{1})_(?P<num>\d{3})_(?P<num2>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tiff)$
avant   : RBX_MED_EPH_C1_L1_001_01_01.tiff
après   : RBX_MED_EPH_C1_L1_001_01_ZA_01.tiff
```

## MED_FLR — 8 487 clés, 55 structure(s)

4 351 clés (51.3%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 952 nom(s) sur 8 487.

**3 525 clés (41.53 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_FLR>_DC_<num:N4>_<num2:N2>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FLR)_DC_(?P<num>\d{4})_(?P<num2>\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>wav)$
avant   : RBX_MED_FLR_DC_1051_01_01.wav
après   : RBX_MED_FLR_DC_1051_01_ZA_01.wav
```

**1 972 clés (23.24 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_FLR>_<num:N2>_<num2:N4>_<lettre:{A|B|C|D}>_<num3:N2>_<seg:{N2}kHz{N2}B>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FLR)_(?P<num>\d{2})_(?P<num2>\d{4})_(?P<lettre>[ABCD])_(?P<num3>\d{2})_(?P<seg>\d{2}kHz\d{2}B))\.(?P<ext>wav)$
exemple : RBX_MED_FLR_17_0094_A_01_44kHz24B.wav
```

**1 098 clés (12.94 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_FLR>_<num:N2>_<num2:N4>_<lettre:{A|B}>_<num3:N2>_<serie:{T|TI|dcn}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FLR)_(?P<num>\d{2})_(?P<num2>\d{4})_(?P<lettre>[AB])_(?P<num3>\d{2})_(?P<serie>(?:T|TI|dcn)))\.(?P<ext>mp3|wav)$
exemple : RBX_MED_FLR_17_0094_A_01_TI.wav
```

**780 clés (9.19 %) — règle directe**

```
gabarit : RBX_MED_FLRS_<serie:{DES|FLR|VER}>_<num:N2>_<num2:N4>_<role:ZA|ZD|ZO>_<vue:N1-2>
regex   : ^(?P<doc>RBX_MED_FLRS_(?P<serie>(?:DES|FLR|VER))_(?P<num>\d{2})_(?P<num2>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{1,2})\.(?P<ext>tif)$
avant   : RBX_MED_FLRS_DES_25_0147_3.tif
après   : RBX_MED_FLRS_DES_25_0147_ZA_3.tif
```

**478 clés (5.63 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_FLR>_DC_<num:N4>_<serie:{back|front}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FLR)_DC_(?P<num>\d{4})_(?P<serie>(?:back|front)))\.(?P<ext>jpg)$
exemple : RBX_MED_FLR_DC_1051_back.jpg
```

**116 clés (1.37 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_MED_<serie:{DES|VER}>_<num:N2>_<num2:N4>_<lettre:{A|B}>_<num3:N2>_<seg:{N2}kHz{N2}B>   (inchangé)
regex   : ^(?P<doc>RBX_MED_(?P<serie>(?:DES|VER))_(?P<num>\d{2})_(?P<num2>\d{4})_(?P<lettre>[AB])_(?P<num3>\d{2})_(?P<seg>\d{2}kHz\d{2}B))\.(?P<ext>wav)$
exemple : RBX_MED_DES_25_0147_A_01_44kHz24B.wav
```

**100 clés (1.18 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_FLR>_<num:N2>_<num2:N4>_<lettre:{A|B}>_<num3:N2>_T_<seg:{N2}kHz{N2}B>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FLR)_(?P<num>\d{2})_(?P<num2>\d{4})_(?P<lettre>[AB])_(?P<num3>\d{2})_T_(?P<seg>\d{2}kHz\d{2}B))\.(?P<ext>wav)$
exemple : RBX_MED_FLR_17_0923_B_02_T_44kHz16B.wav
```

**43 clés (0.51 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_FLR>_<num:N2>_<num2:N4>_<lettre:{A|B}>_<num3:N2>_T_<role:ZA|ZD|ZO>_<vue:N1>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FLR)_(?P<num>\d{2})_(?P<num2>\d{4})_(?P<lettre>[AB])_(?P<num3>\d{2})_T)_(?P<role>Z[ADO])_(?P<vue>\d{1})\.(?P<ext>mp3)$
avant   : RBX_MED_FLR_17_0923_B_02_T_1.mp3
après   : RBX_MED_FLR_17_0923_B_02_T_ZD_1.mp3
```

**40 clés (0.47 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_FLR>_<num:N2>_<num2:N4>_<lettre:{A|B}>_<seg:{N2}kHz{N2}B>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FLR)_(?P<num>\d{2})_(?P<num2>\d{4})_(?P<lettre>[AB])_(?P<seg>\d{2}kHz\d{2}B))\.(?P<ext>wav)$
exemple : RBX_MED_FLR_17_0248_A_44kHz24B.wav
```

**30 clés (0.35 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_FLR>_<num:N2>_<num2:N4>_<niv_B:B{N1}>_TI   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FLR)_(?P<num>\d{2})_(?P<num2>\d{4})_(?P<niv_B>B\d{1})_TI)\.(?P<ext>mp3|wav)$
exemple : RBX_MED_FLR_17_0019_B1_TI.mp3
```

… 45 autre(s) structure(s), 305 clés (détail dans le CSV).

## MED_FOO — 6 clés, 1 structure(s)

6 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**6 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_FOO>_<niv_S:S{N2}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_FOO)_(?P<niv_S>S\d{2})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>JPG|jpg|mp3)$
avant   : RBX_MED_FOO_S01_D01_001.mp3
après   : RBX_MED_FOO_S01_D01_ZD_001.mp3
```

## MED_IMA — 5 821 clés, 6 structure(s)

5 820 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**2 867 clés (49.25 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_IMA>_<niv_CI:CI{N2}>_<niv_L:L{N1}>_<niv_D:D{N3}>_<role:ZA|ZD|ZO>_<vue:N2-3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_IMA)_(?P<niv_CI>CI\d{2})_(?P<niv_L>L\d{1})_(?P<niv_D>D\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2,3})\.(?P<ext>tif)$
avant   : RBX_MED_IMA_CI01_L1_D001_01.tif
après   : RBX_MED_IMA_CI01_L1_D001_ZA_01.tif
```

**1 776 clés (30.51 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_IMA>_CAL_<lettre:{A|B|C}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_IMA)_CAL_(?P<lettre>[ABC])_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_MED_IMA_CAL_A_001_001.jpg
après   : RBX_MED_IMA_CAL_A_001_ZD_001.jpg
```

**637 clés (10.94 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_IMA>_<niv_ETI:ETI{N1}>_<serie:{sigle}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_IMA)_(?P<niv_ETI>ETI\d{1})_(?P<serie>[A-Z]+))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_IMA_ETI1_ADO_001.tif
après   : RBX_MED_IMA_ETI1_ADO_ZA_001.tif
```

**315 clés (5.41 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_IMA>_CA_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_IMA)_CA_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_IMA_CA_D01_001.tif
après   : RBX_MED_IMA_CA_D01_ZA_001.tif
```

**225 clés (3.87 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_IMA>_<niv_BUV:BUV{N1}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_IMA)_(?P<niv_BUV>BUV\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_MED_IMA_BUV1_001.jpg
après   : RBX_MED_IMA_BUV1_ZD_001.jpg
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`IMA_BUV1_001`).

**1 clés (0.02 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_IMA>_<niv_BUV:BUV{N1}>_<seg:{N3}.>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_IMA)_(?P<niv_BUV>BUV\d{1})_(?P<seg>\d{3}\.))\.(?P<ext>tif)$
exemple : RBX_MED_IMA_BUV1_063..tif
```

## MED_JOU — 1 995 clés, 3 structure(s)

1 995 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**1 295 clés (64.91 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_JOU>_<niv_P:P{N5}>_<annee:N4>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_JOU)_(?P<niv_P>P\d{5})_(?P<annee>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_JOU_P20001_1932_000.tif
après   : RBX_MED_JOU_P20001_1932_ZA_000.tif
```

**694 clés (34.79 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_JOU>_<niv_P:P{N5}>_<niv_n:n{N1-4}>_<annee:N4>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_JOU)_(?P<niv_P>P\d{5})_(?P<niv_n>n\d{1,4})_(?P<annee>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_MED_JOU_P50028_n100_1884_01.tif
après   : RBX_MED_JOU_P50028_n100_1884_ZA_01.tif
```

**6 clés (0.3 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_JOU>_<niv_P:P{N5}>_<niv_n:n{N3}a>_<annee:N4>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_JOU)_(?P<niv_P>P\d{5})_(?P<niv_n>n\d{3}a)_(?P<annee>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_MED_JOU_P50028_n163a_1885_01.tif
après   : RBX_MED_JOU_P50028_n163a_1885_ZA_01.tif
```

## MED_LET — 604 clés, 3 structure(s)

570 clés (94.4%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**570 clés (94.37 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_LET>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_LET))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>tif)$
avant   : RBX_MED_LET_0001.tif
après   : RBX_MED_LET_ZA_0001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`Let_0001`).

**32 clés (5.3 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_LET>_<seg:{N3-4}bis>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_LET)_(?P<seg>\d{3,4}bis))\.(?P<ext>tif)$
exemple : RBX_MED_LET_0010bis.tif
```

**2 clés (0.33 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_LET>_<seg:{N4}bis.c>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_LET)_(?P<seg>\d{4}bis\.c))\.(?P<ext>tif)$
exemple : RBX_MED_LET_0056bis.c.tif
```

## MED_MAR — 99 clés, 3 structure(s)

98 clés (99.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**89 clés (89.9 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_MAR>_<num:N3>_<lettre:{A|B|C}>_<num2:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MAR)_(?P<num>\d{3})_(?P<lettre>[ABC])_(?P<num2>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_MAR_001_B_004_001.tif
après   : RBX_MED_MAR_001_B_004_ZA_001.tif
```

**9 clés (9.09 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_MAR>_AFF_<niv_AM:AM{N2}>_<lettre:{B|C}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MAR)_AFF_(?P<niv_AM>AM\d{2})_(?P<lettre>[BC])_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_MAR_AFF_AM01_B_012_001.tif
après   : RBX_MED_MAR_AFF_AM01_B_012_ZA_001.tif
```

**1 clés (1.01 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_MAR>_<num:N3>_A_<num2:N3>_<seg:{N3} - dans aucun train CADN {N4} ou {N4}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MAR)_(?P<num>\d{3})_A_(?P<num2>\d{3})_(?P<seg>\d{3}\ \-\ dans\ aucun\ train\ CADN\ \d{4}\ ou\ \d{4}))\.(?P<ext>tif)$
exemple : RBX_MED_MAR_014_A_001_001 - dans aucun train CADN 2019 ou 2021.tif
```

## MED_MON — 7 553 clés, 8 structure(s)

7 442 clés (98.5%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**3 312 clés (43.85 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_MON>_<serie:{FOL|PLA|RESFOL}>_<num:N4-6>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MON)_(?P<serie>(?:FOL|PLA|RESFOL))_(?P<num>\d{4,6}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif|xml)$
avant   : RBX_MED_MON_FOL_000747_000.tif
après   : RBX_MED_MON_FOL_000747_ZA_000.tif
```

**1 991 clés (26.36 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_MON>_<niv_BR:BR{N1}>_<num:N4>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MON)_(?P<niv_BR>BR\d{1})_(?P<num>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif|xml)$
avant   : RBX_MED_MON_BR8_2709_001.tif
après   : RBX_MED_MON_BR8_2709_ZA_001.tif
```

**1 280 clés (16.95 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_MON>_<num:N1>_<num2:N6>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MON)_(?P<num>\d{1})_(?P<num2>\d{6}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif|xml)$
avant   : RBX_MED_MON_4_210000_001.tif
après   : RBX_MED_MON_4_210000_ZA_001.tif
```

**859 clés (11.37 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_MON>_<niv_RES:RES{N1}>_<num:N4>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MON)_(?P<niv_RES>RES\d{1})_(?P<num>\d{4}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif|xml)$
avant   : RBX_MED_MON_RES8_1192_001.tif
après   : RBX_MED_MON_RES8_1192_ZA_001.tif
```

**76 clés (1.01 %) — METS, nom inchangé**

```
gabarit : RBX_<corpus_code:MED_MON>_<niv_BR:BR{N1}>_<seg:{N4}.mets>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MON)_(?P<niv_BR>BR\d{1})_(?P<seg>\d{4}\.mets))\.(?P<ext>xml)$
exemple : RBX_MED_MON_BR4_0101.mets.xml
```

**18 clés (0.24 %) — METS, nom inchangé**

```
gabarit : RBX_<corpus_code:MED_MON>_<serie:{FOL|PLA|RESFOL}>_<seg:{N4-6}.mets>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MON)_(?P<serie>(?:FOL|PLA|RESFOL))_(?P<seg>\d{4,6}\.mets))\.(?P<ext>xml)$
exemple : RBX_MED_MON_FOL_300462.mets.xml
```

**9 clés (0.12 %) — METS, nom inchangé**

```
gabarit : RBX_<corpus_code:MED_MON>_<num:N1>_<seg:{N6}.mets>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MON)_(?P<num>\d{1})_(?P<seg>\d{6}\.mets))\.(?P<ext>xml)$
exemple : RBX_MED_MON_4_207931.mets.xml
```

**8 clés (0.11 %) — METS, nom inchangé**

```
gabarit : RBX_<corpus_code:MED_MON>_<niv_RES:RES{N1}>_<seg:{N4}.mets>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MON)_(?P<niv_RES>RES\d{1})_(?P<seg>\d{4}\.mets))\.(?P<ext>xml)$
exemple : RBX_MED_MON_RES4_0012.mets.xml
```

## MED_MS — 32 414 clés, 4 structure(s)

32 240 clés (99.5%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**32 240 clés (99.46 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_MS>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3-4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MS)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3,4})\.(?P<ext>tif)$
avant   : RBX_MED_MS_006_0000.tif
après   : RBX_MED_MS_006_ZA_0000.tif
```

**170 clés (0.52 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_MS>_<num:N3>_<num2:N4>_A   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MS)_(?P<num>\d{3})_(?P<num2>\d{4})_A)\.(?P<ext>tif)$
exemple : RBX_MED_MS_007_0000_A.tif
```

**3 clés (0.01 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_MS>_<num:N3>_<num2:N3>_<niv_New:New{N1}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MS)_(?P<num>\d{3})_(?P<num2>\d{3})_(?P<niv_New>New\d{1}))\.(?P<ext>tif)$
exemple : RBX_MED_MS_306_027_New1.tif
```

**1 clés (0.0 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_MS>_<num:N3>_<seg:{N3}a>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_MS)_(?P<num>\d{3})_(?P<seg>\d{3}a))\.(?P<ext>tif)$
exemple : RBX_MED_MS_076_061a.tif
```

## MED_PAR — 18 223 clés, 18 structure(s)

18 020 clés (98.9%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 6 535 nom(s) sur 18 223.

**6 588 clés (36.15 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PAR>_<serie:{BOH|BOU|NIE}>_<serie2:{JAC|MJO|SPO|TUR}>_<num:N3-4>_<role:ZA|ZD|ZO>_<vue:N2-3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PAR)_(?P<serie>(?:BOH|BOU|NIE))_(?P<serie2>(?:JAC|MJO|SPO|TUR))_(?P<num>\d{3,4}))_(?P<role>Z[ADO])_(?P<vue>\d{2,3})\.(?P<ext>tif)$
avant   : RBX_MED_PAR_BOH_SPO_114_01.tif
après   : RBX_MED_PAR_BOH_SPO_114_ZA_01.tif
```

**3 177 clés (17.43 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PAR>_<serie:{BOH|BOU|LAT|RAI}>_<serie2:{DES|DOC|RES|TAP|TEX}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PAR)_(?P<serie>(?:BOH|BOU|LAT|RAI))_(?P<serie2>(?:DES|DOC|RES|TAP|TEX))_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PAR_BOH_RES_D01_001.tif
après   : RBX_MED_PAR_BOH_RES_D01_ZA_001.tif
```

**2 877 clés (15.79 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_PAR_LAB_<niv_PH:PH{N2}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_AMR_PAR_LAB_(?P<niv_PH>PH\d{2})_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg|tif|tiff)$
avant   : RBX_AMR_PAR_LAB_PH01_001_01.jpg
après   : RBX_AMR_PAR_LAB_PH01_001_ZD_01.jpg
```
À noter : 17 sous-vue(s) avérée(s).

**1 661 clés (9.11 %) — règle directe**

```
gabarit : <serie:{MED|RBX}>_<serie2:{MED|PAR}>_<serie3:{CAR|PAR}>_<serie4:{CHO|FOO|TUR}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<serie>(?:MED|RBX))_(?P<serie2>(?:MED|PAR))_(?P<serie3>(?:CAR|PAR))_(?P<serie4>(?:CHO|FOO|TUR))_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : MED_PAR_CAR_CHO_D01_001.tif
après   : MED_PAR_CAR_CHO_D01_ZA_001.tif
```

**1 621 clés (8.9 %) — règle directe**

```
gabarit : RBX_AMR_PAR_LAB_<niv_PH:PH{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_PAR_LAB_(?P<niv_PH>PH\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_AMR_PAR_LAB_PH02_001.jpg
après   : RBX_AMR_PAR_LAB_PH02_ZD_001.jpg
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`LAB_PH02_001`) ; 5 fichier(s) de document dont le dernier nombre n'est pas une vue.

**884 clés (4.85 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PAR>_TER_<niv_CP:CP{N2}>_<role:ZA|ZD|ZO>_<vue:N4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PAR)_TER_(?P<niv_CP>CP\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{4})\.(?P<ext>JPG|jpg)$
avant   : RBX_MED_PAR_TER_CP01_0001.jpg
après   : RBX_MED_PAR_TER_CP01_ZD_0001.jpg
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`TER_CP01_0001`).

**316 clés (1.73 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PAR>_PRE_CP_<niv_CP:CP{N2}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PAR)_PRE_CP_(?P<niv_CP>CP\d{2})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_MED_PAR_PRE_CP_CP00_D01_01.tif
après   : RBX_MED_PAR_PRE_CP_CP00_D01_ZA_01.tif
```

**268 clés (1.47 %) — règle directe, sous-vue probable à vérifier**

```
gabarit : RBX_AMR_PAR_LES_<niv_D:D{N2}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_AMR_PAR_LES_(?P<niv_D>D\d{2})_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_AMR_PAR_LES_D01_001_001.tif
après   : RBX_AMR_PAR_LES_D01_001_ZA_001.tif
```

**176 clés (0.97 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PAR>_<serie:{CHI|DED|TRI}>_<serie2:{AMI|CAT}>_<serie3:{AFF|ALB|DOC|PHO|PRE}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N2-3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PAR)_(?P<serie>(?:CHI|DED|TRI))_(?P<serie2>(?:AMI|CAT))_(?P<serie3>(?:AFF|ALB|DOC|PHO|PRE))_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2,3})\.(?P<ext>mp4|tif|wmv)$
avant   : RBX_MED_PAR_DED_CAT_DOC_D01_01.tif
après   : RBX_MED_PAR_DED_CAT_DOC_D01_ZA_01.tif
```

**159 clés (0.87 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PAR>_PRE_PHO_<niv_PHO:PHO{N2}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PAR)_PRE_PHO_(?P<niv_PHO>PHO\d{2})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_MED_PAR_PRE_PHO_PHO00_D01_01.tif
après   : RBX_MED_PAR_PRE_PHO_PHO00_D01_ZA_01.tif
```

… 8 autre(s) structure(s), 496 clés (détail dans le CSV).

## MED_PBI — 1 084 clés, 1 structure(s)

1 084 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**1 084 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PBI>_LAB_<serie:{MEL|ROU|TRA}>_<serie2:{sigle}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N2-3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PBI)_LAB_(?P<serie>(?:MEL|ROU|TRA))_(?P<serie2>[A-Z]+)_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{2,3})\.(?P<ext>tif)$
avant   : RBX_MED_PBI_LAB_MEL_ANN_001_01.tif
après   : RBX_MED_PBI_LAB_MEL_ANN_001_ZA_01.tif
```

## MED_PER — 1 596 clés, 2 structure(s)

1 532 clés (96.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**1 532 clés (95.99 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PER>_<niv_P:P{N5}>_<num:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PER)_(?P<niv_P>P\d{5})_(?P<num>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif|xml)$
avant   : RBX_MED_PER_P20475_19020802_001.tif
après   : RBX_MED_PER_P20475_19020802_ZA_001.tif
```

**64 clés (4.01 %) — METS, nom inchangé**

```
gabarit : RBX_<corpus_code:MED_PER>_<niv_P:P{N5}>_<seg:{N8}.mets>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PER)_(?P<niv_P>P\d{5})_(?P<seg>\d{8}\.mets))\.(?P<ext>xml)$
exemple : RBX_MED_PER_P20475_19020802.mets.xml
```

## MED_PHD — 1 946 clés, 12 structure(s)

1 945 clés (99.9%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**547 clés (28.11 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_URB:URB{N2}>_<niv_S:S{N2}>_<lettre:{A|B|H}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_URB>URB\d{2})_(?P<niv_S>S\d{2})_(?P<lettre>[ABH])_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_URB01_S01_A_D01_001.tif
après   : RBX_MED_PHD_URB01_S01_A_D01_ZA_001.tif
```

**435 clés (22.35 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_SOC:SOC{N2}>_<niv_S:S{N2}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_SOC>SOC\d{2})_(?P<niv_S>S\d{2})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_SOC01_S01_D01_001.tif
après   : RBX_MED_PHD_SOC01_S01_D01_ZA_001.tif
```

**343 clés (17.63 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_URB:URB{N2}>_<niv_S:S{N2}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_URB>URB\d{2})_(?P<niv_S>S\d{2})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_URB01_S01_D01_001.tif
après   : RBX_MED_PHD_URB01_S01_D01_ZA_001.tif
```

**260 clés (13.36 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_SOC:SOC{N2}>_<niv_S:S{N2}>_<lettre:{A|B|H}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_SOC>SOC\d{2})_(?P<niv_S>S\d{2})_(?P<lettre>[ABH])_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_SOC01_S01_A_D01_001.tif
après   : RBX_MED_PHD_SOC01_S01_A_D01_ZA_001.tif
```

**134 clés (6.89 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_ETA:ETA{N2}>_<niv_S:S{N2}>_<lettre:{A|B|H}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_ETA>ETA\d{2})_(?P<niv_S>S\d{2})_(?P<lettre>[ABH])_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_ETA01_S01_A_D01_001.tif
après   : RBX_MED_PHD_ETA01_S01_A_D01_ZA_001.tif
```

**66 clés (3.39 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_EVE:EVE{N2}>_<lettre:{A|B|H}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_EVE>EVE\d{2})_(?P<lettre>[ABH])_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_EVE01_A_D01_001.tif
après   : RBX_MED_PHD_EVE01_A_D01_ZA_001.tif
```

**44 clés (2.26 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_EVE:EVE{N2}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_EVE>EVE\d{2})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_EVE01_D01_001.tif
après   : RBX_MED_PHD_EVE01_D01_ZA_001.tif
```

**43 clés (2.21 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_ETA:ETA{N2}>_<niv_S:S{N2}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_ETA>ETA\d{2})_(?P<niv_S>S\d{2})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_ETA01_S02_D01_001.tif
après   : RBX_MED_PHD_ETA01_S02_D01_ZA_001.tif
```

**34 clés (1.75 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_SOC:SOC{N2}>_<niv_S:S{N2}>_<niv_D:D{N2}>_<lettre:{A|B|C}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_SOC>SOC\d{2})_(?P<niv_S>S\d{2})_(?P<niv_D>D\d{2})_(?P<lettre>[ABC]))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_SOC03_S01_D03_B_001.tif
après   : RBX_MED_PHD_SOC03_S01_D03_B_ZA_001.tif
```

**24 clés (1.23 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHD>_<niv_GUE:GUE{N2}>_<lettre:{A|B}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHD)_(?P<niv_GUE>GUE\d{2})_(?P<lettre>[AB])_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHD_GUE01_A_D01_001.tif
après   : RBX_MED_PHD_GUE01_A_D01_ZA_001.tif
```

… 2 autre(s) structure(s), 16 clés (détail dans le CSV).

## MED_PHO — 1 338 clés, 3 structure(s)

1 338 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**917 clés (68.54 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHO>_DIA_<niv_DEL:DEL{N1}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHO)_DIA_(?P<niv_DEL>DEL\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>TIF|tif)$
avant   : RBX_MED_PHO_DIA_DEL1_001.tif
après   : RBX_MED_PHO_DIA_DEL1_ZA_001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`PHO_DIA_DEL1_001`).

**275 clés (20.55 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHO>_PNU_<niv_DUP:DUP{N2}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHO)_PNU_(?P<niv_DUP>DUP\d{2})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>JPG|jpg|png)$
avant   : RBX_MED_PHO_PNU_DUP01_D01_01.jpg
après   : RBX_MED_PHO_PNU_DUP01_D01_ZD_01.jpg
```

**146 clés (10.91 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PHO>_DIA_<niv_SAU:SAU{N1}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PHO)_DIA_(?P<niv_SAU>SAU\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PHO_DIA_SAU1_001.tif
après   : RBX_MED_PHO_DIA_SAU1_ZA_001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`PHO_DIA_SAU1_001`).

## MED_PLA — 274 clés, 2 structure(s)

273 clés (99.6%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**273 clés (99.64 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PLA>_<lettre:{A|B|C}>_<num:N3>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PLA)_(?P<lettre>[ABC])_(?P<num>\d{3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MED_PLA_A_004_001.tif
après   : RBX_MED_PLA_A_004_ZA_001.tif
```

**1 clés (0.36 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_PLA>_A_<num:N3>_<num2:N3>_tres_haute_def   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PLA)_A_(?P<num>\d{3})_(?P<num2>\d{3})_tres_haute_def)\.(?P<ext>tif)$
exemple : RBX_MED_PLA_A_029_001_tres_haute_def.tif
```

## MED_PUB — 1 274 clés, 1 structure(s)

1 274 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**1 274 clés (100.0 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_PUB>_<serie:{ETA|PAT}>_<serie2:{BIB|FDP}>_<num:N2-3>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_PUB)_(?P<serie>(?:ETA|PAT))_(?P<serie2>(?:BIB|FDP))_(?P<num>\d{2,3}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>tif)$
avant   : RBX_MED_PUB_ETA_BIB_01_01.tif
après   : RBX_MED_PUB_ETA_BIB_01_ZA_01.tif
```

## MED_VDM — 38 211 clés, 21 structure(s)

34 962 clés (91.5%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 426 nom(s) sur 38 211.

**33 720 clés (88.25 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_VDM>_<sous_serie:{sigle}{N1-3}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N2-4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_VDM)_(?P<sous_serie>[A-Z]+\d{1,3})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2,4})\.(?P<ext>TIF|tif)$
avant   : RBX_MED_VDM_ADP1_D01_001.tif
après   : RBX_MED_VDM_ADP1_D01_ZA_001.tif
```

**2 692 clés (7.05 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_VDM>_<sous_serie:{sigle}{N1-3}>_<niv_D:D{N2}>_<seg:{N3}{sigle}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_VDM)_(?P<sous_serie>[A-Z]+\d{1,3})_(?P<niv_D>D\d{2})_(?P<seg>\d{3}[A-Za-z]+))\.(?P<ext>TIF|tif)$
exemple : RBX_MED_VDM_ADP1_D01_025a.tif
```

**1 182 clés (3.09 %) — règle directe**

```
gabarit : RBX_<corpus_code:MED_VDM>_<serie:{PER|PHO|VCA}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N2-4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_VDM)_(?P<serie>(?:PER|PHO|VCA))_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2,4})\.(?P<ext>TIF|tif)$
avant   : RBX_MED_VDM_PER_D01_001.tif
après   : RBX_MED_VDM_PER_D01_ZA_001.tif
```

**232 clés (0.61 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_MED_I_VDM_<niv_RES:RES{N1}>_<num:N2-4>_<num2:N1-3>_A   (inchangé)
regex   : ^(?P<doc>RBX_MED_I_VDM_(?P<niv_RES>RES\d{1})_(?P<num>\d{2,4})_(?P<num2>\d{1,3})_A)\.(?P<ext>tif)$
exemple : RBX_MED_I_VDM_RES4_276_117_A.tif
```

**119 clés (0.31 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_MED_I_VDM_RESFOL_<num:N3>_<num2:N1-2>_A   (inchangé)
regex   : ^(?P<doc>RBX_MED_I_VDM_RESFOL_(?P<num>\d{3})_(?P<num2>\d{1,2})_A)\.(?P<ext>tif)$
exemple : RBX_MED_I_VDM_RESFOL_220_10_A.tif
```

**75 clés (0.2 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_VDM>_<niv_PDH:PDH{N1}>_<niv_D:D{N2}>_<seg:{N3} >   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_VDM)_(?P<niv_PDH>PDH\d{1})_(?P<niv_D>D\d{2})_(?P<seg>\d{3}\ ))\.(?P<ext>tif)$
exemple : RBX_MED_VDM_PDH1_D01_001 .tif
```

**64 clés (0.17 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MED_VDM>_<serie:{PER|PHO|VCA}>_<niv_D:D{N2}>_<seg:{N3}{a|b|c|d|e}>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MED_VDM)_(?P<serie>(?:PER|PHO|VCA))_(?P<niv_D>D\d{2})_(?P<seg>\d{3}[abcde]))\.(?P<ext>TIF|tif)$
exemple : RBX_MED_VDM_PER_D07_059a.tif
```

**36 clés (0.09 %) — règle directe**

```
gabarit : <corpus_code:MED_VDM>_<niv_TEM:TEM{N1}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>MED_VDM)_(?P<niv_TEM>TEM\d{1})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : MED_VDM_TEM1_D03_002.tif
après   : MED_VDM_TEM1_D03_ZA_002.tif
```

**22 clés (0.06 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_MED_I_VDM_<niv_RES:RES{N1}>_<num:N4>_<num2:N1>_<num3:N2-3>_A   (inchangé)
regex   : ^(?P<doc>RBX_MED_I_VDM_(?P<niv_RES>RES\d{1})_(?P<num>\d{4})_(?P<num2>\d{1})_(?P<num3>\d{2,3})_A)\.(?P<ext>tif)$
exemple : RBX_MED_I_VDM_RES8_5040_1_136_A.tif
```

**17 clés (0.04 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_MED_I_VDM_<niv_LP:LP{N1}>_<num:N6>_<num2:N2-3>_A   (inchangé)
regex   : ^(?P<doc>RBX_MED_I_VDM_(?P<niv_LP>LP\d{1})_(?P<num>\d{6})_(?P<num2>\d{2,3})_A)\.(?P<ext>tif)$
exemple : RBX_MED_I_VDM_LP4_208647_115_A.tif
```

… 11 autre(s) structure(s), 52 clés (détail dans le CSV).

## MUS_ARC — 318 clés, 10 structure(s)

315 clés (99.1%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**82 clés (25.79 %) — règle directe**

```
gabarit : RBX_<corpus_code:MUS_ARC>_<serie:{EINF|IND}>_<num:N3-6>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_(?P<serie>(?:EINF|IND))_(?P<num>\d{3,6}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg|tif)$
avant   : RBX_MUS_ARC_EINF_008_01.tif
après   : RBX_MUS_ARC_EINF_008_ZA_01.tif
```
À noter : 53 sous-vue(s) avérée(s).

**75 clés (23.58 %) — règle directe**

```
gabarit : RBX_<corpus_code:MUS_ARC>_FDT_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N2>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_FDT_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{2})\.(?P<ext>jpg|tif)$
avant   : RBX_MUS_ARC_FDT_D01_01.tif
après   : RBX_MUS_ARC_FDT_D01_ZA_01.tif
```

**70 clés (22.01 %) — règle directe**

```
gabarit : RBX_<corpus_code:MUS_ARC>_<niv_CAV:CAV{N1}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_(?P<niv_CAV>CAV\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_MUS_ARC_CAV2_001.jpg
après   : RBX_MUS_ARC_CAV2_ZD_001.jpg
```

**36 clés (11.32 %) — règle directe**

```
gabarit : RBX_<corpus_code:MUS_ARC>_<niv_GDU:GDU{N1}>_<niv_D:D{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_(?P<niv_GDU>GDU\d{1})_(?P<niv_D>D\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_MUS_ARC_GDU1_D01_001.tif
après   : RBX_MUS_ARC_GDU1_D01_ZA_001.tif
```

**22 clés (6.92 %) — règle directe**

```
gabarit : RBX_<corpus_code:MUS_ARC>_<niv_PIS:PIS{N1}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_(?P<niv_PIS>PIS\d{1}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_MUS_ARC_PIS1_003.tif
après   : RBX_MUS_ARC_PIS1_ZA_003.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`MUS_ARC_PIS1_003`).

**21 clés (6.6 %) — règle directe**

```
gabarit : <corpus_code:MUS_ARC>_<niv_GDU:GDU{N1}>_<niv_D:D{N2-3}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>MUS_ARC)_(?P<niv_GDU>GDU\d{1})_(?P<niv_D>D\d{2,3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg)$
avant   : MUS_ARC_GDU3_D011_001.jpg
après   : MUS_ARC_GDU3_D011_ZD_001.jpg
```

**9 clés (2.83 %) — règle directe**

```
gabarit : RBX_<corpus_code:MUS_ARC>_<serie:{EINF|HDV}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_(?P<serie>(?:EINF|HDV)))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_MUS_ARC_EINF_001.tif
après   : RBX_MUS_ARC_EINF_ZA_001.tif
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`MUS_ARC_EINF_001`) ; 1 fichier(s) de document dont le dernier nombre n'est pas une vue.

**1 clés (0.31 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MUS_ARC>_EINF_<seg:{N3}A>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_EINF_(?P<seg>\d{3}A))\.(?P<ext>tif)$
exemple : RBX_MUS_ARC_EINF_008A.tif
```

**1 clés (0.31 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MUS_ARC>_EINF_<seg:{N3}B>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_EINF_(?P<seg>\d{3}B))\.(?P<ext>tif)$
exemple : RBX_MUS_ARC_EINF_001B.tif
```

**1 clés (0.31 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:MUS_ARC>_EINF_<num:N3>_<seg:{N2} >   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>MUS_ARC)_EINF_(?P<num>\d{3})_(?P<seg>\d{2}\ ))\.(?P<ext>jpg)$
exemple : RBX_MUS_ARC_EINF_008_20 .jpg
```

## MUS_VAI — 1 918 clés, 1 structure(s)

1 918 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus est absent de 1 918 nom(s) sur 1 918.

**1 918 clés (100.0 %) — règle directe**

```
gabarit : RBX_MUS_<niv_VAI:VAI{N1}>_<niv_D:D{N2}>_<niv_F:F{N2-3}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_MUS_(?P<niv_VAI>VAI\d{1})_(?P<niv_D>D\d{2})_(?P<niv_F>F\d{2,3}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_MUS_VAI1_D01_F01_001.jpg
après   : RBX_MUS_VAI1_D01_F01_ZD_001.jpg
```

## OBS_JOU — 1 757 clés, 5 structure(s)

1 756 clés (99.9%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**1 680 clés (95.62 %) — règle directe**

```
gabarit : RBX_<corpus_code:OBS_JOU>_<serie:{ENT|GAZ|HOM|QLI|VPC}>_<num:N3-4>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>OBS_JOU)_(?P<serie>(?:ENT|GAZ|HOM|QLI|VPC))_(?P<num>\d{3,4}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>jpg|tif)$
avant   : RBX_OBS_JOU_ENT_009_001.jpg
après   : RBX_OBS_JOU_ENT_009_ZD_001.jpg
```

**42 clés (2.39 %) — règle directe**

```
gabarit : RBX_<corpus_code:OBS_JOU>_<serie:{QLI|VPC}>_<role:ZA|ZD|ZO>_<vue:N3-4>
regex   : ^(?P<doc>RBX_(?P<corpus_code>OBS_JOU)_(?P<serie>(?:QLI|VPC)))_(?P<role>Z[ADO])_(?P<vue>\d{3,4})\.(?P<ext>pdf)$
avant   : RBX_OBS_JOU_QLI_001.pdf
après   : RBX_OBS_JOU_QLI_ZD_001.pdf
```
À noter : l'indicateur s'insère à l'intérieur de l'identifiant du ref (`OBS_JOU_QLI_001`).

**30 clés (1.71 %) — règle directe**

```
gabarit : RBX_<corpus_code:OBS_JOU>_VPC_<seg:{N3}a>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>OBS_JOU)_VPC_(?P<seg>\d{3}a))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_OBS_JOU_VPC_010a_001.tif
après   : RBX_OBS_JOU_VPC_010a_ZA_001.tif
```

**4 clés (0.23 %) — règle directe**

```
gabarit : RBX_<corpus_code:OBS_JOU>_VPC_<seg:{N3}b>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>RBX_(?P<corpus_code>OBS_JOU)_VPC_(?P<seg>\d{3}b))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : RBX_OBS_JOU_VPC_039b_001.tif
après   : RBX_OBS_JOU_VPC_039b_ZA_001.tif
```

**1 clés (0.06 %) — sans numéro de vue : à trancher**

```
gabarit : RBX_<corpus_code:OBS_JOU>_VPC_<seg:{N4}a>   (inchangé)
regex   : ^(?P<doc>RBX_(?P<corpus_code>OBS_JOU)_VPC_(?P<seg>\d{4}a))\.(?P<ext>pdf)$
exemple : RBX_OBS_JOU_VPC_0036a.pdf
```

## PRA_AVE — 176 340 clés, 1 structure(s)

176 340 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**176 340 clés (100.0 %) — règle directe**

```
gabarit : <corpus_code:PRA_AVE>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_AVE)_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_AVE_18880921_001.pdf
après   : PRA_AVE_18880921_ZD_001.pdf
```

## PRA_BDR — 178 clés, 1 structure(s)

178 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**178 clés (100.0 %) — règle directe**

```
gabarit : <corpus_code:PRA_BDR>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_BDR)_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>tif)$
avant   : PRA_BDR_19171117_001.tif
après   : PRA_BDR_19171117_ZA_001.tif
```

## PRA_CRT — 116 806 clés, 1 structure(s)

116 806 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**116 806 clés (100.0 %) — règle directe**

```
gabarit : <corpus_code:PRA_CRT>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_CRT)_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_CRT_19010501_001.pdf
après   : PRA_CRT_19010501_ZD_001.pdf
```

## PRA_CTG — 5 752 clés, 1 structure(s)

5 752 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**5 752 clés (100.0 %) — règle directe**

```
gabarit : <corpus_code:PRA_CTG>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_CTG)_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_CTG_19000415_001.pdf
après   : PRA_CTG_19000415_ZD_001.pdf
```

## PRA_ERT — 377 078 clés, 2 structure(s)

377 076 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**377 076 clés (100.0 %) — règle directe**

```
gabarit : <corpus_code:PRA_ERT>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_ERT)_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_ERT_18950502_001.pdf
après   : PRA_ERT_18950502_ZD_001.pdf
```

**2 clés (0.0 %) — sans numéro de vue : à trancher**

```
gabarit : <corpus_code:PRA_ERT>_<date:N8>_<seg:{N3} >   (inchangé)
regex   : ^(?P<doc>(?P<corpus_code>PRA_ERT)_(?P<date>\d{8})_(?P<seg>\d{3}\ ))\.(?P<ext>pdf|txt)$
exemple : PRA_ERT_19411015_004 .pdf
```

## PRA_IND — 64 400 clés, 1 structure(s)

64 400 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**64 400 clés (100.0 %) — règle directe**

```
gabarit : <corpus_code:PRA_IND>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_IND)_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_IND_18400101_001.pdf
après   : PRA_IND_18400101_ZD_001.pdf
```

## PRA_JRX — 595 291 clés, 3 structure(s)

595 291 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**587 572 clés (98.7 %) — règle directe**

```
gabarit : <corpus_code:PRA_JRX>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_JRX)_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_JRX_18560618_001.pdf
après   : PRA_JRX_18560618_ZD_001.pdf
```

**7 703 clés (1.29 %) — règle directe**

```
gabarit : <corpus_code:PRA_JRX>_<date:N8>_<serie:{ES|M|S}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_JRX)_(?P<date>\d{8})_(?P<serie>(?:ES|M|S)))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_JRX_18980903_M_001.pdf
après   : PRA_JRX_18980903_M_ZD_001.pdf
```

**16 clés (0.0 %) — règle directe**

```
gabarit : <corpus_code:PRA_JRX>_<seg:{N8}-{N2}>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_JRX)_(?P<seg>\d{8}\-\d{2}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_JRX_18950524-25_001.pdf
après   : PRA_JRX_18950524-25_ZD_001.pdf
```

## PRA_RTG — 3 367 clés, 1 structure(s)

3 367 clés (100.0%) reçoivent l'indicateur par la règle directe. Le code du corpus figure dans tous les noms.

**3 367 clés (100.0 %) — règle directe**

```
gabarit : <corpus_code:PRA_RTG>_<date:N8>_<role:ZA|ZD|ZO>_<vue:N3>
regex   : ^(?P<doc>(?P<corpus_code>PRA_RTG)_(?P<date>\d{8}))_(?P<role>Z[ADO])_(?P<vue>\d{3})\.(?P<ext>pdf|tif|txt|xml)$
avant   : PRA_RTG_18910517_001.pdf
après   : PRA_RTG_18910517_ZD_001.pdf
```
