import sqlite3
import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse


class DataExporter:
    def __init__(self, db_path='pesca_database.db'):
        self.db_path = db_path

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def calculate_series(self):
        with self.get_connection() as conn:
            df_prod = pd.read_sql_query("SELECT * FROM produttivita", conn)
            df_and_occ = pd.read_sql_query("SELECT * FROM andamento_occupazione", conn)
            df_imp_econ = pd.read_sql_query("SELECT * FROM importanza_economica", conn)
            df_agg_regions = pd.read_sql_query("SELECT * FROM regioni_aggregate", conn)

        df_prod_with_area = pd.merge(df_prod, df_agg_regions, on='Regione')
        print (df_prod_with_area)
        df_and_occ_with_area = pd.merge(df_and_occ, df_agg_regions, on='Regione')
        df_imp_econ_with_area = pd.merge(df_imp_econ, df_agg_regions, on='Regione')

        # Serie 1: Produttività totale per le 5 aree
        series1 = df_prod_with_area.groupby(['Anno', 'Area'])['Produttività in migliaia di euro'].sum().reset_index()
        #print (series1) OK

        # Serie 2: Produttività totale nazionale
        series2 = df_prod_with_area.groupby('Anno')['Produttività in migliaia di euro'].sum().reset_index()
        #print(series2) OK

        # Serie 3: Media percentuale valore aggiunto pesca piscicoltura per le 5 aree
        series3 = df_imp_econ_with_area.groupby(['Anno', 'Area'])['Percentuale valore aggiunto pesca-piscicoltura-servizi'].mean().reset_index()
        print(series3)

        # Serie 4: Media variazione percentuale occupazione nazionale
        series4 = df_and_occ_with_area.groupby('Anno')['Variazione percentuale unità di lavoro della pesca'].mean().reset_index()
        print(series4)

        # Serie 5: Media variazione percentuale occupazione per le 5 aree
        series5 = df_and_occ_with_area.groupby(['Anno', 'Area'])['Variazione percentuale unità di lavoro della pesca'].mean().reset_index()
        print(series5)
        # Salva le serie calcolate nella tabella SQLite
        self.save_series_to_db(series1, series2, series3, series4, series5)

        return series1, series2, series3, series4, series5

    def save_series_to_db(self, series1, series2, series3, series4, series5):
        with self.get_connection() as conn:
            for _, row in series1.iterrows():
                conn.execute("""
                    INSERT OR REPLACE INTO series_calcolate (anno, area, produttivita_totale)
                    VALUES (?, ?, ?)
                    """, (row['Anno'], row['Area'], row['Produttività in migliaia di euro']))

            for _, row in series2.iterrows():
                conn.execute("""
                    UPDATE series_calcolate
                    SET produttivita_totale_nazionale = ?
                    WHERE anno = ?
                    """, (row['Produttività in migliaia di euro'], row['Anno']))

            for _, row in series3.iterrows():
                conn.execute("""
                    UPDATE series_calcolate
                    SET media_percentuale_valore_aggiunto_piscicoltura = ?
                    WHERE anno = ? AND area = ?
                    """, (row['Percentuale valore aggiunto pesca-piscicoltura-servizi'], row['Anno'], row['Area']))

            for _, row in series4.iterrows():
                conn.execute("""
                    UPDATE series_calcolate
                    SET media_variazione_percentuale_occupazione_nazionale = ?
                    WHERE anno = ?
                    """, (row['Variazione percentuale unità di lavoro della pesca'], row['Anno']))

            for _, row in series5.iterrows():
                conn.execute("""
                    UPDATE series_calcolate
                    SET media_variazione_percentuale_occupazione = ?
                    WHERE anno = ? AND area = ?
                    """, (row['Variazione percentuale unità di lavoro della pesca'], row['Anno'], row['Area']))

            conn.commit()

    # FUNZIONE DI EXPORT FUNZIONA
    def export_data(self, table_name, da_anno, a_anno, file_path):
        query = f"SELECT * FROM {table_name} WHERE Anno BETWEEN {da_anno} AND {a_anno}"
        with self.get_connection() as conn:
            df = pd.read_sql_query(query, conn)
        df.to_csv(file_path, index=False)
        return file_path


