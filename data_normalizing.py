import pandas as pd

class DataProcessor:
    def __init__(self):
        pass

    def replace_null_values_with_mean(self, df):
        # Sostituisci i valori nulli con la media della terza colonna
        df.iloc[:, 2] = df.iloc[:, 2].fillna(df.iloc[:, 2].astype(float).mean())
        return df

    def replace_null_values_with_median(self, df):
        # Sostituisci i valori nulli con la mediana della terza colonna
        df.iloc[:, 2] = df.iloc[:, 2].fillna(df.iloc[:, 2].astype(float).median())
        return df

    def replace_null_values_with_mean_dynamic(self, df):
        # Sostituisci i valori nulli con la media di ogni colonna
        for column in df.columns:
            if df[column].dtype in ['float64', 'int64']:
                df[column] = df[column].fillna(df[column].mean())
        return df

    def replace_null_values_with_median_dynamic(self, df):
        # Sostituisci i valori nulli con la mediana di ogni colonna
        for column in df.columns:
            if df[column].dtype in ['float64', 'int64']:
                df[column] = df[column].fillna(df[column].median())
        return df

    def create_data_for_year(self, df, year):
        # Crea i dati relativi all'anno specificato
        df_data_year = pd.DataFrame(df.groupby("Regione")["Variazione percentuale unità di lavoro della pesca"].mean()).reset_index()
        df_data_year.insert(0, "Anno", year)
        return df_data_year

    def filter_by_column_value(self, df, column, value):
        # Filtra il DataFrame per una colonna specifica e un valore specifico
        return df[df[column] == value]

    def calculate_statistics(self, df, column):
        # Calcola statistiche descrittive per una colonna specifica
        return df[column].describe()

    def remove_outliers(self, df, column):
        # Rimuovi outlier da una colonna specifica utilizzando il metodo IQR
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        return df[~((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]

    def normalize_column(self, df, column):
        # Normalizza una colonna specifica
        df[column] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())
        return df

    def add_calculated_column(self, df, new_column_name, col1, col2, operation):
        # Aggiungi una nuova colonna calcolata a partire da altre due colonne
        if operation == 'add':
            df[new_column_name] = df[col1] + df[col2]
        elif operation == 'subtract':
            df[new_column_name] = df[col1] - df[col2]
        elif operation == 'multiply':
            df[new_column_name] = df[col1] * df[col2]
        elif operation == 'divide':
            df[new_column_name] = df[col1] / df[col2]
        return df

    def get_unique_values(self, df, column):
        # Ottieni i valori unici da una colonna specifica
        return df[column].unique()
