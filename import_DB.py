import pandas as pd
import requests
import chardet
from rich.jupyter import display

# URL dei file CSV
produttivita_url = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Produttivita-del-settore-della-pesca-per-regione.csv"
importanza_economica_url = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Importanza-economica-del-settore-della-pesca-per-regione.csv"
andamento_occupazione_url = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Andamento-occupazione-del-settore-della-pesca-per-regione.csv"

# Funzione per rilevare la codifica e leggere i dati
def read_csv_with_encoding(url):
    response = requests.get(url)
    raw_data = response.content
    result = chardet.detect(raw_data)
    encoding = result['encoding']
    print(f"Encoding rilevata per {url}: {encoding}")
    return pd.read_csv(url, encoding=encoding, sep=";")

# Leggi i dati
df_prod = read_csv_with_encoding(produttivita_url)
df_imp_econ = read_csv_with_encoding(importanza_economica_url)
df_and_occ = read_csv_with_encoding(andamento_occupazione_url)
print(df_prod)