from oaipmh_scythe import Scythe
import pandas as pd
from os.path import join, isfile
from os import listdir
import re
from datetime import datetime
import argparse
import concurrent.futures

# Date d'horodatage des fichiers produits. Par défaut aujourd'hui ; passer
# --date AAAAMMJJ pour aligner le moissonnage sur le NEW_REF_DATE d'un run ref
# (cf. scripts/azrael/_pipeline.py) et chaîner avec oai_join_ref.py.
parser = argparse.ArgumentParser(description="Moissonnage OAI-PMH BnR")
parser.add_argument("--date", default=datetime.today().strftime('%Y%m%d'),
                    help="Horodatage des fichiers de sortie (AAAAMMJJ, défaut: aujourd'hui)")
args = parser.parse_args()
date = args.date

baseURL = 'http://oai.bn-r.fr/oai.php'
scythe = Scythe(baseURL)

sets = scythe.list_sets(resumption_token=None)
data = []
for set in sets:
    fields = ['setSpec', 'setName'] #, 'description']
    result = {}
    for field in fields :
        if hasattr(set, field):
            result[field] = getattr(set, field)
    data.append(result)
sets_df = pd.DataFrame(data)


res = []
for setname in sets_df['setSpec']:
    identifiers = scythe.list_identifiers(from_=None, until=None,
                                          metadata_prefix='oai_dc',
                                          set_=setname,
                                          resumption_token=None, ignore_deleted=False)
    data = []
    try:
        for identifier in identifiers:
            fields = ['identifier'] #, 'description']
            result = {}
            for field in fields :
                if hasattr(identifier, field):
                    result[field] = getattr(identifier, field)
            data.append(result)
        identifiers_df = pd.DataFrame(data)
        identifiers_df['setname'] = setname
        res_file = join("data", "oai", "sets_identifiers", f"identifiers_{date}_{setname}.csv.gz")
        #print(identifiers_df)
        identifiers_df.to_csv(res_file, index=False)
        #print({set : len(identifiers_df)})
        res.append({'setSpec': setname, 'nb_notices': len(identifiers_df)})
    except:
        res.append({'setSpec': setname, 'nb_notices': 0})
        print(f"Erreur pour {setname}")
#print(pd.DataFrame(res))


# In[4]:


sets_size_df = pd.DataFrame(res)
sets_df = sets_df.merge(sets_size_df, how='outer', on='setSpec')
sets_df = sets_df.sort_values(by='nb_notices', ascending=False)
sets_df.to_csv(join("results", "oai", f"bnr_sets_{date}.csv"), index=False)

setnames = [set for set in sets_df[sets_df['nb_notices'] > 0]['setSpec']]

def get_selected_metadata(setname, identifier):
        selected_metadata = {'setname': setname, 'identifier': identifier, 'cote': None, 'title': None}
        try:
            record = scythe.get_record(identifier, metadata_prefix='oai_dc')
            metadata = record.metadata
            selected_metadata = {'identifier': identifier, 'cote': None, 'title': None}
            if "source" in metadata:
                source = metadata['source']
                source = source[0]
                cote = re.sub(r".*,\s", "", source)
                selected_metadata['cote'] = cote
            if "title" in metadata:
                title = " ; ".join(metadata['title'])
                selected_metadata['title'] = title
            return(selected_metadata)
        except:
            #data.append(selected_metadata)
            print(f"Pb sur notice {identifier}")


# In[7]:


# liste dynamique : tous les sets réellement moissonnés (nb_notices > 0),
# au lieu d'une liste figée qui ignorait silencieusement les nouveaux sets.
for setname in setnames:
    print(setname)
    i = 0
    data =[]
    res_file = join("data", "oai", "sets_identifiers", f"identifiers_{date}_{setname}.csv.gz")
    if not isfile(res_file):
        # nb_notices > 0 mais fichier d'identifiants absent (échec à l'étape 2) :
        # on ne plante pas tout le run, on signale et on passe.
        print(f"  identifiants absents pour {setname}, set ignoré")
        continue
    identifiers_df = pd.read_csv(res_file)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for identifier in identifiers_df['identifier']:
            futures.append(executor.submit(get_selected_metadata, setname=setname, identifier=identifier))

        for future in concurrent.futures.as_completed(futures):
            #i += 1
            #if i % 500 == 0:
            #    print(f"-- {i}")
            data.append(future.result())

    data2 = [d for d in data if d is not None]
    data_df = pd.DataFrame(data2)
    data_df.to_csv(join("data", "oai", "sets_records", f"records_{date}_{setname}.csv.gz"), index=False)

ifiles = [f for f in listdir(join("data", "oai", "sets_identifiers")) if date in f]
ifiles_df = []
for f in ifiles:
    df_ = pd.read_csv(join("data", "oai", "sets_identifiers", f))
    ifiles_df.append(df_)
i_df = pd.concat(ifiles_df)

rfiles = [f for f in listdir(join("data", "oai", "sets_records")) if date in f]
rfiles_df = []
for f in rfiles:
    df_ = pd.read_csv(join("data", "oai", "sets_records", f))
    rfiles_df.append(df_)
r_df = pd.concat(rfiles_df)

df = r_df.merge(i_df, on="identifier", how='outer')
df['osiros_id'] = df['identifier'].str.replace("oai:bn-r.fr:", "BNR")
df.to_csv(join("data", "oai", f"oai_records_{date}.csv.gz"), index=False)
