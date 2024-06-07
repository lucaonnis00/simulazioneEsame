import pandas as pd

class DataProcessor:
    def __init__(self):
        pass

    def replace_null_values_with_mean(self, df):
        # Sostituisci i valori nulli con la media della terza colonna
        df.iloc[:, 2] = df.iloc[:, 2].fillna(df.iloc[:, 2].astype(float).mean())
        return df

    def create_data_for_year(self, df, year):
        # Crea i dati relativi all'anno specificato
        df_data_year = pd.DataFrame(df.groupby("Regione")["Variazione percentuale unità di lavoro della pesca"].mean()).reset_index()
        df_data_year.insert(0, "Anno", year)
        return df_data_year