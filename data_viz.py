import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
class ChartPlotter:
    def __init__(self, db_name):
        self.db_name = db_name

    def get_connection(self):
        return sqlite3.connect(self.db_name)

    def plot_bar_chart(self, table_name):
        try:
            # Connessione SQLite creata dinamicamente
            with self.get_connection() as conn:
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

    def close_connection(self):
        pass  # Puoi implementare un metodo per chiudere la connessione se necessario