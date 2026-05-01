from oaipmh_scythe import Scythe
import pandas as pd
from os.path import join
from os import listdir
import re
from datetime import datetime
import concurrent.futures

date = datetime.today().strftime('%Y%m%d')
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


for setname in [
     'RBX_MED_AFF',
     'RBX_MED_PAR',
     'RBX_MED_CP',
     'RBX_AMR_VIC',
     'RBX_MED_DIL',
     'RBX_AMR_AMD',
     'RBX_MED_PIA',
     'RBX_MED_MEU',
     'RBX_MED_IMA',
     'RBX_AMR_AME',
     'RBX_AMR_AFF',
     'RBX_MED_PHO',
     'RBX_MED_VDM',
     'RBX_MED_EPH',
     'RBX_MED_PRO',
     'RBX_AMR_AMK',
     'RBX_MED_LET',
     'RBX_OBS_JOU',
     'RBX_MUS_VAI',
     'RES_WEB',
     'RBX_AMR_PR',
     'RBX_MED_MS',
     'DEPOT_PUBLIC',
     'RBX_AMR_2F1',
     'RBX_MED_CHA',
     'RBX_AMR_AMF',
     'RBX_MED_PUB',
     'RBX_AMR_GUE',
     'RBX_MED_CAT',
     'RBX_MED_PLA',
     'RBX_MED_FLR',
     'RBX_AMR_LEB',
     'RBX_AMR_AMR',
     'RBX_AMR_DEL',
     'RBX_ARA_CPS',
     'RBX_MUS_ARC',
     'RBX_MED_MAR',
     'RBX_AMR_PLA',
     'RBX_LAI',
     'RBX_AMR_OBJ',
     'RBX_AMR_RAM',
     'RBX_VAH_PUB',
     'RBX_AMR_PHO',
     'RBX_LAR_PUB',
     'RBX_MED_MON',
     'RBX_CSV_PAL',
     'RBX_AMR_CAD',
     'RBX_MDF_MTX',
     'RBX_MED_COM',
     'RBX_AMR_PUV',
     'RBX_MED_FOO',
     'RBX_PRA_RTG',
     'RBX_PRA_CTG',
     'RBX_PRA_ERT',
     'RBX_PRA_AVE',
     'RBX_PRA_JRX',
     'RBX_PRA_CRT',
     'RBX_PRA_IND',
]:
    print(setname)
    i = 0
    data =[]
    res_file = join("data", "oai", "sets_identifiers", f"identifiers_{date}_{setname}.csv.gz")
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
df.to_csv(join("data", "oai", f"oai_records_{date}.csv.gz"), index=False)
