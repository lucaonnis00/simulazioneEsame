import sqlite3
from http.client import HTTPException
import pandas as pd
from fastapi import FastAPI, Query
from starlette.responses import FileResponse
from DataExporter import DataExporter
from data_viz import ChartPlotter

app = FastAPI()
data_exporter = DataExporter('pesca_database.db')
data_viz = ChartPlotter('pesca_database.db')
#FUNZIONA
@app.get("/table/{table_name}")
def get_table(table_name: str, da_anno: int = Query(..., description="Anno di inizio"),
              a_anno: int = Query(..., description="Anno di fine")):
    if a_anno < da_anno:
        raise HTTPException(status_code=400, detail="A ANNO deve essere maggiore o uguale a DA ANNO")

    conn = sqlite3.connect('pesca_database.db')
    query = f"SELECT * FROM {table_name} WHERE Anno BETWEEN {da_anno} AND {a_anno}"

    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return df.to_dict(orient="records")

#FUNZIONA
@app.get("/export/{table_name}")
def export_table(table_name: str, da_anno: int = Query(..., description="Anno di inizio"), a_anno: int = Query(..., description="Anno di fine")):
    try:
        file_path = f"{table_name}_{da_anno}_{a_anno}.csv"
        exported_file = data_exporter.export_data(table_name, da_anno, a_anno, file_path)
        return FileResponse(path=exported_file, filename=file_path, media_type='text/csv')
    except Exception as e:
        return {"error": str(e)}

@app.get("/calculate_series")
def calculate_series():
    try:
        series1, series2, series3, series4, series5 = data_exporter.calculate_series()
        return {
            "series1": series1.to_dict(orient='records'),
            "series2": series2.to_dict(orient='records'),
            "series3": series3.to_dict(orient='records'),
            "series4": series4.to_dict(orient='records'),
            "series5": series5.to_dict(orient='records')
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/plot/{table_name}")
def plotta(table_name: str):
    data_viz.plot_bar_chart(table_name=table_name)