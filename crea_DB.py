import sqlite3
import pandas as pd

class crea_DB:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)

    def create_table(self, df, table_name):
        self.conn = sqlite3.connect(self.db_name)
        df.to_sql(table_name, self.conn, if_exists='replace', index=False)
        self.conn.close()

    def create_table_from_query(self, query, table_name):
        self.conn = sqlite3.connect(self.db_name)
        df = pd.read_sql_query(query, self.conn)
        df.to_sql(table_name, self.conn, if_exists='replace', index=False)
        self.conn.close()
    def query_and_print(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    def close_connection(self):
        self.conn.close()