from rbx_s3 import Rbx_client
import pandas as pd
from os import walk
from os.path import join, sep, splitext
import csv
from datetime import datetime

import concurrent.futures
from tqdm import tqdm

input_folder = join('results', 's3', 'transfert_cadn_314-1000')
#prefix = "\\\\srvbnr.ntrbx.local\BNR"
#prefix = "/home/kibini/bnr"
prefix = "/media/fpichenot/CADN_165-0500/2018.06.101_N - VILLE DE ROUBAIX"

USER = 'user_rw'
bucket = 'mediatheque-patarch-communicable'
rbx_client = Rbx_client(user=USER)


# On crée une liste de dictionnaires en entrée de la fonction rbx_upload_file :
def get_files2upload(data_file_path, rbx_client, bucket, prefix):
    files2upload = []

    #print(data_file_path)
    data2upload = pd.read_csv(data_file_path)
    for file in data2upload.to_dict(orient='records'):
        file_path = file['path'].replace("/", sep)
        if prefix:
            file['file_name'] = join(prefix, file_path, file['name'])
        else:
            file['file_name'] = join(file_path, file['name'])

        tags = {
            'uuid': file['uuid'],
            'checksum_md5': file['checksum_md5']
        }
        file['tags_str'] = "&".join(f"{key}={value}" for key, value in tags.items())

        file['client'] = rbx_client
        file['bucket'] = bucket


        files2upload.append(file)

    return files2upload

# Fonction d'upload
def rbx_upload_file(file_data):
    print(file_data['file_name'])

    res2log = {
        'name': file_data['name'],
        'path': file_data['path'],
        'checksum_md5': file_data['checksum_md5'],
        'uuid': file_data['uuid'],
        'key': file_data['s3_key'],
        'size': file_data['size'],
        'uploaded': False,
        'uploaded_file_size': None,
        'uploaded_file_lastmodified': None,
        'error': None
    }

    try:
        upload_res = rbx_client.upload(file_data['file_name'],
                               file_data['bucket'],
                               file_data['s3_key'],
                               ExtraArgs = {"Tagging": file_data['tags_str']})

        res2log['uploaded'] = upload_res['result']
        if 'error' in upload_res:
            res2log['error'] = str(upload_res['error'])
        if 'LastModified' in upload_res:
            res2log['uploaded_file_lastmodified'] = upload_res['LastModified']
        if 'size' in upload_res:
            res2log['uploaded_file_size'] = upload_res['size']
            if res2log['uploaded_file_size'] != res2log['size']:
                res2log['error'] = 'cohérence tailles'
    except Exception as e:
        # Un fichier en erreur ne doit jamais interrompre l'upload des autres fichiers du lot
        res2log['error'] = f"exception non gérée : {e}"

    return(res2log)

# Exécution
files2proceed = []
for dir_path, dirs, files in walk(input_folder):
    for file in files:
        if file != '.gitkeep':
            file_path = join(dir_path, file)
            filename = splitext(file)[0]
            files2proceed.append([file_path, filename])

for file_info in files2proceed:
    data_file_name = file_info[1]
    print(data_file_name)
    dt = datetime.now().strftime("%Y%m%d%H%M%S")
    result_file = join("results", "s3", "result", f"{data_file_name}_upload_{dt}.csv")
    with open(result_file, 'w', newline='') as logfile:
        fieldnames = ['name', 'path', 'checksum_md5', 'uuid', 'size', 'key', 'uploaded',
                      'uploaded_file_size', 'uploaded_file_lastmodified', 'error']
        writer = csv.DictWriter(logfile, fieldnames=fieldnames)
        writer.writeheader()

        files2upload = get_files2upload(file_info[0], rbx_client, bucket, prefix)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for file_data in files2upload:
                futures.append(executor.submit(rbx_upload_file, file_data=file_data))
                #print(file_data)

            #progress_bar = tqdm(total=len(files2upload), desc="Processing")
            #for future in tqdm(concurrent.futures.as_completed(futures)):
            i = 0
            n_ok = 0
            n_ko = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    res2log = future.result()
                except Exception as e:
                    # Ne devrait pas arriver (rbx_upload_file capture déjà ses erreurs),
                    # mais on ne veut jamais perdre le reste du lot pour un thread en échec.
                    res2log = {
                        'name': None, 'path': None, 'checksum_md5': None, 'uuid': None,
                        'key': None, 'size': None, 'uploaded': False,
                        'uploaded_file_size': None, 'uploaded_file_lastmodified': None,
                        'error': f"exception thread : {e}"
                    }

                writer.writerow(res2log)
                logfile.flush()

                i += 1
                if res2log.get('uploaded'):
                    n_ok += 1
                else:
                    n_ko += 1
                print(f"{i}/{len(files2upload)} (ok={n_ok}, ko={n_ko})")

        print(f"{data_file_name} : terminé — {n_ok} ok / {n_ko} en erreur (log : {result_file})")
