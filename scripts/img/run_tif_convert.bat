@echo off
setlocal EnableExtensions EnableDelayedExpansion
rem ===========================================================================
rem run_tif_convert.bat - Lanceur de tests pour tif_convert.py.
rem
rem Pour chaque dossier de corpus, convertit RECURSIVEMENT les TIF ranges sous
rem   <corpus>\conservation\...
rem vers
rem   <corpus>\diffusion\...        (arborescence reproduite a l'identique)
rem Le TYPE de document - donc le FACTEUR de reduction - est DEDUIT DU NOM du
rem dossier de corpus (ex. corpus_presse_..._1 -> presse), et on balaie plusieurs
rem qualites (Q) et plusieurs seuils de resolution minimale (--resolution-min).
rem
rem Q, facteur et seuil etant inscrits dans le nom de sortie
rem (ex. page001_q85_f65_rmin2000.jpg), toutes les combinaisons cohabitent dans le
rem meme dossier sans s'ecraser, et une relance saute ce qui est deja produit.
rem
rem Usage :
rem   run_tif_convert.bat                    rem traite la liste DEFAULT_INPUT_DIRS
rem   run_tif_convert.bat DIR1 DIR2 ...      rem traite les dossiers passes en argument
rem
rem Les dossiers passes en argument (utilises par chaine_corpus.bat) priment sur
rem la liste DEFAULT_INPUT_DIRS codee plus bas.
rem
rem Prerequis : env conda << rbx-bnr-data >> (env unifie du projet, support JP2).
rem ===========================================================================

rem ---------------------------------------------------------------------------
rem Configuration - a adapter
rem ---------------------------------------------------------------------------

rem Dossiers d'entree par defaut (chacun contient un sous-dossier conservation\).
rem Utilises seulement si aucun dossier n'est passe en argument.
set DEFAULT_INPUT_DIRS="E:\corpus\corpus_presse_20260502_1" "E:\corpus\corpus_presse_20260502_2" "E:\corpus\corpus_presse_20260502_3"

rem Sous-dossier source (ou sont les TIF) et sous-dossier de sortie (miroir).
set "SOURCE_SUBDIR=conservation"
set "OUT_SUBDIR=diffusion"

rem Format de sortie : jp2 | jpeg | ptiff
set "FORMAT=jpeg"

rem Qualites (Q) a tester.
set "QUALITIES=80 85 90"

rem Seuils de resolution minimale (largeur en px) a tester.
set "RESMINS=2000 2500 3000"

rem Nombre de processus paralleles (vide = nb de coeurs).
set "WORKERS="

rem Env conda du projet (unifie, inclut libvips/pyvips avec support JP2).
set "CONDA_ENV=rbx-bnr-data"

rem Chemin du script Python (a cote de ce lanceur par defaut).
set "SCRIPT_DIR=%~dp0"
set "PY_SCRIPT=%SCRIPT_DIR%tif_convert.py"

rem ---------------------------------------------------------------------------
rem Execution
rem ---------------------------------------------------------------------------

if not exist "%PY_SCRIPT%" ( echo Script introuvable : %PY_SCRIPT% 1>&2 & exit /b 1 )

rem Dossiers a traiter : arguments positionnels s'il y en a, sinon la liste defaut.
if "%~1"=="" ( set "INPUT_DIRS=%DEFAULT_INPUT_DIRS%" ) else ( set "INPUT_DIRS=%*" )

rem Nombre de qualites (NQ) et de seuils (NR) testes.
set /a NQ=0
for %%q in (%QUALITIES%) do set /a NQ+=1
set /a NR=0
for %%r in (%RESMINS%) do set /a NR+=1

rem Decompte des essais reellement lancables (type reconnu + sous-dossier
rem conservation\ present) x Q x resolution-min.
set /a total=0
for %%d in (%INPUT_DIRS%) do (
  if exist "%%~d\%SOURCE_SUBDIR%\" (
    call :type_de "%%~nxd"
    if not errorlevel 1 set /a total+=NQ*NR
  )
)

set /a n=0
echo === !total! essai(s) a lancer (format=%FORMAT%) ===

for %%d in (%INPUT_DIRS%) do (
  set "in_dir=%%~d"
  set "base=%%~nxd"
  if not exist "!in_dir!\" (
    echo !! Dossier d'entree introuvable, ignore : !in_dir! 1>&2
  ) else (
    set "src=!in_dir!\%SOURCE_SUBDIR%"
    if not exist "!src!\" (
      echo !! Sous-dossier source absent, ignore : !src! 1>&2
    ) else (
      call :type_de "!base!"
      if errorlevel 1 (
        echo !! Type indetermine pour "!base!" ^(aucune cle parmi : manuscrits_plans iconographie presse^), ignore. 1>&2
      ) else (
        set "out=!in_dir!\%OUT_SUBDIR%"
        for %%q in (%QUALITIES%) do (
          for %%r in (%RESMINS%) do (
            set /a n+=1
            echo.
            echo --- [!n!/!total!] !base! [!DOCTYPE!] : Q=%%q, f=!FACTEUR!, resolution-min=%%r
            echo     !src!  -^>  !out!
            rem CSV recapitulatif distinct par essai (nom horodate a la milliseconde).
            for /f %%s in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss_fff"') do set "stamp=%%s"
            set "CSV=!out!\recap_q%%q_rmin%%r_!stamp!.csv"
            if defined WORKERS (
              conda run -n %CONDA_ENV% python "%PY_SCRIPT%" "!src!" "!out!" --format %FORMAT% --quality %%q --facteur !FACTEUR! --resolution-min %%r --csv "!CSV!" --workers %WORKERS%
            ) else (
              conda run -n %CONDA_ENV% python "%PY_SCRIPT%" "!src!" "!out!" --format %FORMAT% --quality %%q --facteur !FACTEUR! --resolution-min %%r --csv "!CSV!"
            )
            rem On n'interrompt pas le balayage si un essai echoue.
            if errorlevel 1 echo !! Essai en erreur ^(!DOCTYPE!, Q=%%q, resolution-min=%%r^) - on continue. 1>&2
          )
        )
      )
    )
  )
)

echo.
echo === Termine. Un CSV horodate par essai a la racine de chaque dossier de sortie. ===
endlocal
exit /b 0

rem ---------------------------------------------------------------------------
rem :type_de "<basename>"  -> deduit le type de document a partir du nom du
rem dossier de corpus. Definit DOCTYPE et FACTEUR et renvoie 0 si une cle
rem correspond ; renvoie 1 sinon. Facteurs : manuscrits/plans=haut (0.80),
rem iconographie=moyen (0.65), presse=bas (0.50).
rem ---------------------------------------------------------------------------
:type_de
set "DOCTYPE="
set "FACTEUR="
echo %~1 | findstr /c:"manuscrits_plans" >nul && ( set "DOCTYPE=manuscrits_plans" & set "FACTEUR=0.80" & exit /b 0 )
echo %~1 | findstr /c:"iconographie"     >nul && ( set "DOCTYPE=iconographie"     & set "FACTEUR=0.65" & exit /b 0 )
echo %~1 | findstr /c:"presse"           >nul && ( set "DOCTYPE=presse"           & set "FACTEUR=0.50" & exit /b 0 )
exit /b 1
