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
from  data_reading import DataLoader
from crea_DB import crea_DB
from data_viz import ChartPlotter
from data_normalizing import DataProcessor
from DataAnalyzer import DataAnalyzer

#istanze classi
chart_plotter = ChartPlotter('pesca_database.db')
data_processor = DataProcessor()
db_manager = crea_DB('pesca_database.db')

# URL dei dati
produttivita = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Produttivita-del-settore-della-pesca-per-regione.csv"
importanza_economica = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Importanza-economica-del-settore-della-pesca-per-regione.csv"
andamento_occupazione = "https://raw.githubusercontent.com/lucaonnis00/simulazioneEsame/main/Andamento-occupazione-del-settore-della-pesca-per-regione.csv"

prod_loader = DataLoader(produttivita)
imp_econ_loader = DataLoader(importanza_economica)
and_occ_loader = DataLoader(andamento_occupazione)

#creazione dataframe da csv
df_prod = prod_loader.carica_dati()
df_imp_econ = imp_econ_loader.carica_dati()
df_and_occ = and_occ_loader.carica_dati()


# Verifico la presenza di valori nulli
print("Valori nulli in df_prod prima della sostituzione:")
print(df_prod.isnull().sum())
print("Valori nulli in df_imp_econ prima della sostituzione:")
print(df_imp_econ.isnull().sum())
print("Valori nulli in df_and_occ prima della sostituzione:")
print(df_and_occ.isnull().sum())

# Sostituisci i valori nulli con la media
df_prod = data_processor.replace_null_values_with_mean(df_prod)
df_and_occ = data_processor.replace_null_values_with_mean(df_and_occ)

print("Valori nulli in df_prod dopo la sostituzione:")
print(df_prod.isnull().sum())
print("Valori nulli in df_and_occ dopo la sostituzione:")
print(df_and_occ.isnull().sum())

# Creare i dati del 2000 relativi alla variazione percentuale di lavoro della pesca con la media ottenuta dagli anni successivi
df_data_2000 = data_processor.create_data_for_year(df_and_occ, 2000)
df_and_occ = pd.concat([df_data_2000, df_and_occ], ignore_index=True)
print("DataFrame df_and_occ dopo l'inserimento dei dati del 2000:")
print(df_and_occ)

# Crea una connessione al database SQLite (crea il database se non esiste)
db_manager.create_table(df_prod, 'produttivita')
db_manager.create_table(df_imp_econ, 'importanza_economica')
db_manager.create_table(df_and_occ, 'andamento_occupazione')

# Verifica che le tabelle siano state create correttamente
#db_manager.query_and_print("SELECT * FROM produttivita;")
#db_manager.query_and_print("SELECT * FROM importanza_economica;")
#db_manager.query_and_print("SELECT * FROM andamento_occupazione;")

# Chiudi la connessione al database
db_manager.close_connection()

# Plot del grafico a barre per la tabella 'importanza_economica'
#chart_plotter.plot_bar_chart('importanza_economica')
#chart_plotter.plot_bar_chart('produttivita')

# Chiusura della connessione al database
# chart_plotter.close_connection()

# Creazione di un'istanza della classe DataAnalyzer
data_analyzer = DataAnalyzer(df_prod, df_and_occ)

regioni_aggr = data_analyzer.aggregate_regions()

print(regioni_aggr)

db_manager.create_table(regioni_aggr, 'regioni_aggregate')


