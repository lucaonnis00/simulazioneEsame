import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import chardet
import sqlite3
# Scarica i dati dalla URL

produttivita = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Produttivita-del-settore-della-pesca-per-regione.csv"
importanza_economica = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Importanza-economica-del-settore-della-pesca-per-regione.csv"
andamento_occupazione = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Andamento-occupazione-del-settore-della-pesca-per-regione.csv"

response = requests.get(produttivita)
raw_data = response.content

# Rileva la codifica dei dati
result = chardet.detect(raw_data)
encoding = result['encoding']
print(encoding)

# Leggi i dati utilizzando la codifica rilevata
df_prod = pd.read_csv(produttivita,encoding=encoding, sep=";")
df_imp_econ = pd.read_csv(importanza_economica, encoding=encoding, sep=";")
df_and_occ = pd.read_csv(andamento_occupazione, encoding=encoding, sep=";")
print(df_prod.head())
print(df_imp_econ.head())
print(df_and_occ)

#verifico la presenza di valori nulli
print(df_prod.isnull().sum())
print(df_imp_econ.isnull().sum())
print(df_and_occ.isnull().sum())

# Sostituisci i valori nulli con la media della terza colonna .iloc[:,2] seleziona tutte le righe della terza colonna
df_prod.iloc[:, 2] = df_prod.iloc[:, 2].fillna(df_prod.iloc[:, 2].astype(float).mean())
print("Valori nulli in df_prod dopo la sostituzione:")
print(df_prod.isnull().sum())

df_and_occ.iloc[:, 2] = df_and_occ.iloc[:, 2].fillna(df_and_occ.iloc[:, 2].astype(float).mean())
print("Valori nulli in df_and_occ dopo la sostituzione:")
print(df_and_occ.isnull().sum())

#creare i dati del 2000 relativi alla variazione percentuale di lavoro della pesca con la media ottenuta dagli anni successivi
#reset index resetta gli indici del nuovo df e fa in modo che la colonna regione corrisponda all'indice 0
df_data_2000 = pd.DataFrame(df_and_occ.groupby("Regione")["Variazione percentuale unità di lavoro della pesca"].mean()).reset_index()
print(df_data_2000)
df_data_2000.insert(0,"Anno", 2000)

df_and_occ = pd.concat([df_data_2000, df_and_occ], ignore_index=True)
print(df_and_occ)

# Definisci una funzione per eseguire una query e visualizzare i risultati come DataFrame
def query_and_print(query):
    result = pd.read_sql_query(query, conn)
    print(result)

# Crea una connessione al database SQLite (crea il database se non esiste)
conn = sqlite3.connect('pesca_database.db')

# Importa i DataFrame nelle tabelle SQLite
df_prod.to_sql('produttivita', conn, if_exists='replace', index=False)
df_imp_econ.to_sql('importanza_economica', conn, if_exists='replace', index=False)
df_and_occ.to_sql('andamento_occupazione', conn, if_exists='replace', index=False)

# Verifica che le tabelle siano state create correttamente
query_and_print("SELECT * FROM produttivita;")
query_and_print("SELECT * FROM importanza_economica;")
query_and_print("SELECT * FROM andamento_occupazione;")

# Chiudi la connessione al database
conn.close()

def plot_bar_chart(table_name):
    # Connetti al database SQLite
    conn = sqlite3.connect('pesca_database.db')

    try:
        # Leggi i dati dalla tabella specificata
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)

        # Plotta un grafico a barre
        plt.figure(figsize=(10, 6))
        plt.bar(df['Regione'], df.iloc[:, 2])
        plt.xlabel('Regione')
        plt.ylabel('Valore')
        plt.title(f'Dati dalla tabella {table_name}')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Si è verificato un errore: {e}")
    finally:
        # Chiudi la connessione al database
        conn.close()

# Esempio di utilizzo
plot_bar_chart('importanza_economica')