import pandas as pd
import os

from bnr.azrael import Jhove2csv

all_results = []
files = [f for f os.listdir('data') if f[:5] == 'jhove' and f[-6:] == 'xml.gz']
for filename in sorted(files):
    print(filename)
    chunk_id = filename[12:29]
    try:
        jhove2csv = Jhove2csv(jhove_file= f"data/{filename}")
        jhove2csv.jhove_parser()
        jhove2csv.save_results(f"data/{filename[:-6]}csv")
        jhove2csv.results2df(chunk_id=chunk_id)
        all_results.append(jhove2csv.results_df)
    except:
        print(f"Pb sur fichier {filename}")
all_results_df = pd.concat(all_results)
all_results_df.to_csv("data/jhove_synth.csv.gz", index=False)
