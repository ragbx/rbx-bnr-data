@echo off
setlocal EnableExtensions EnableDelayedExpansion
rem ===========================================================================
rem chaine_corpus.bat - Chaine complete : TELECHARGEMENT puis CONVERSION.
rem
rem 1) Telecharge TOUS les corpus listes dans CSV_DIR (manifestes .csv/.csv.gz)
rem    via telechargement_corpus.py, vers <DEST>\<stem>\conservation\...
rem 2) SEULEMENT une fois tous les telechargements termines, lance la conversion
rem    via run_tif_convert.bat sur ces memes dossiers (<DEST>\<stem>), qui produit
rem    <DEST>\<stem>\diffusion\...
rem
rem Les dossiers de conversion sont DEDUITS des manifestes (stem du CSV ->
rem <DEST>\<stem>) : telechargement et conversion traitent donc exactement les
rem memes corpus, sans liste a maintenir en double.
rem
rem Usage :
rem   chaine_corpus.bat
rem
rem Prerequis : env conda << rbx-bnr-data >> (env unifie du projet, support JP2).
rem ===========================================================================

rem ---------------------------------------------------------------------------
rem Configuration - a adapter
rem ---------------------------------------------------------------------------

rem Env conda du projet (unifie, inclut libvips/pyvips avec support JP2).
set "CONDA_ENV=rbx-bnr-data"

rem Repertoire des manifestes a traiter (defaut du telechargeur : results\corpus\tif2dl).
set "CSV_DIR=results\corpus\tif2dl"

rem Racine du stockage source (ou path+name se resolvent). A ADAPTER au montage local.
set "SOURCE=\\srvbnr.ntrbx.local\BNR"

rem Racine de destination sur le disque dur. C'est aussi la racine lue par la
rem conversion : <DEST>\<stem>\conservation -> <DEST>\<stem>\diffusion.
set "DEST=E:\corpus"

rem ---------------------------------------------------------------------------
rem Execution
rem ---------------------------------------------------------------------------

set "SCRIPT_DIR=%~dp0"
set "DL_SCRIPT=%SCRIPT_DIR%telechargement_corpus.py"
set "CONV_SCRIPT=%SCRIPT_DIR%run_tif_convert.bat"

if not exist "%DL_SCRIPT%" ( echo Script introuvable : %DL_SCRIPT% 1>&2 & exit /b 1 )
if not exist "%CONV_SCRIPT%" ( echo Script introuvable : %CONV_SCRIPT% 1>&2 & exit /b 1 )
if not exist "%CSV_DIR%\" ( echo Repertoire de manifestes introuvable : %CSV_DIR% 1>&2 & exit /b 1 )

rem --- Etape 1 : telecharger TOUS les corpus avant toute conversion -----------
echo === [1/2] Telechargement de tous les corpus de %CSV_DIR% vers %DEST% ===
conda run -n %CONDA_ENV% python "%DL_SCRIPT%" --csv-dir "%CSV_DIR%" --source "%SOURCE%" --dest "%DEST%"
if errorlevel 1 ( echo Echec du telechargement, conversion annulee. 1>&2 & exit /b 1 )

rem --- Dossiers a convertir : deduits des manifestes (stem -> <DEST>\<stem>) ---
rem SEEN sert au dedoublonnage (un meme corpus present en .csv et .csv.gz).
set "INPUT_DIRS="
set "SEEN=|"

for %%f in ("%CSV_DIR%\*.csv") do call :add_stem "%%~nf"
for %%f in ("%CSV_DIR%\*.csv.gz") do (
  set "stem=%%~nf"
  set "stem=!stem:.csv=!"
  call :add_stem "!stem!"
)

if not defined INPUT_DIRS (
  echo Aucun manifeste ^(.csv/.csv.gz^) dans %CSV_DIR% : rien a convertir. 1>&2
  exit /b 1
)

rem --- Etape 2 : conversion des corpus telecharges ----------------------------
echo.
echo === [2/2] Conversion des corpus telecharges ===
call "%CONV_SCRIPT%"%INPUT_DIRS%

echo.
echo === Chaine terminee. ===
endlocal
exit /b 0

rem ---------------------------------------------------------------------------
rem :add_stem "<stem>"  -> ajoute "<DEST>\<stem>" a INPUT_DIRS si pas deja vu.
rem ---------------------------------------------------------------------------
:add_stem
set "_s=%~1"
echo !SEEN! | findstr /c:"|!_s!|" >nul && exit /b 0
set "SEEN=!SEEN!!_s!|"
set "INPUT_DIRS=!INPUT_DIRS! "%DEST%\!_s!""
exit /b 0
