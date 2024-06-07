import requests
import chardet
import pandas as pd

class DataLoader:
    def __init__(self, url):
        self.url = url
        self.encoding = self._rileva_encoding()

    def _rileva_encoding(self):
        # Rileva la codifica dei dati
        response = requests.get(self.url)
        raw_data = response.content
        result = chardet.detect(raw_data)
        return result['encoding']

    def carica_dati(self):
        # Leggi i dati utilizzando la codifica rilevata
        df = pd.read_csv(self.url, encoding=self.encoding, sep=";")
        # Stampa le prime righe del DataFrame per confermare il caricamento
        print(df.head())
        return df

