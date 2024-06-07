import pandas as pd

class DataAnalyzer:
    def __init__(self, df_prod, df_and_occ):
        self.df_prod = df_prod
        self.df_and_occ = df_and_occ

    def aggregate_regions(self):
        region_mapping = {
            'Nord-ovest': ["Valle d'Aosta", "Piemonte", "Liguria", "Lombardia"],
            'Nord-est': ["Trentino-Alto Adige", "Veneto", "Friuli-Venezia Giulia", "Emilia-Romagna"],
            'Centro': ["Toscana", "Umbria", "Marche", "Lazio", "Abruzzo"],
            'Sud': ["Molise", "Campania", "Puglia", "Basilicata", "Calabria"],
            'Isole': ["Sicilia", "Sardegna"]
        }

        df_agg_regions = pd.DataFrame(columns=['Regione', 'Area'])

        for area, region_list in region_mapping.items():
            area_df = pd.DataFrame({'Regione': region_list, 'Area': area})
            df_agg_regions = pd.concat([df_agg_regions, area_df], ignore_index=True)

        return df_agg_regions


    def calculate_series(self):
        # Aggregazione delle regioni
        df_agg_regions = self.aggregate_regions()

        # Unione dei dati di produttività con le regioni aggregate
        df_prod_with_area = pd.merge(self.df_prod, df_agg_regions, on='Regione')

        # Serie 1: Produttività totale per le 5 aree
        series1 = df_prod_with_area.groupby('Area')['Valore'].sum()

        # Serie 2: Produttività totale nazionale
        series2 = df_prod_with_area['Valore'].sum()

        # Serie 3: Media percentuale valore aggiunto pesca piscicoltura per le 5 aree
        series3 = df_prod_with_area.groupby('Area')['Percentuale valore aggiunto pesca piscicoltura'].mean()

        # Serie 4: Media variazione percentuale occupazione nazionale
        series4 = self.df_and_occ['Variazione percentuale occupazione'].mean()

        # Serie 5: Media variazione percentuale occupazione per le 5 aree
        df_and_occ_with_area = pd.merge(self.df_and_occ, df_agg_regions, on='Regione')
        series5 = df_and_occ_with_area.groupby('Area')['Variazione percentuale occupazione'].mean()

        return series1, series2, series3, series4, series5