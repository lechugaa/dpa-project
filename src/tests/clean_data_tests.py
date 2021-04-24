import unittest
import pandas as pd

from src.pipeline.limpieza_feature_eng import DataCleaner
from src.utils.general import load_from_pickle, get_file_path, get_pickle_from_s3_to_pandas, load_from_pickle


class CleanTest(unittest.TestCase):

    def __init__(self, historic, query_date, *args, **kwargs):
        super(CleanTest, self).__init__(*args, **kwargs)
        self.historic = historic
        self.query_date = query_date

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=DataCleaner.prefix)
        self.clean_df = load_from_pickle(file_path)

        ingestion_path = get_file_path(historic=self.historic, query_date=self.query_date)
        ingestion_data = load_from_pickle(ingestion_path)
        self.original_df = pd.DataFrame.from_dict(ingestion_data)
        
    def test_df_smaller_equal(self):
        assert self.clean_df.shape[1] <= self.original_df.shape[1], "Added extra columns!"

    def test_df_not_empty(self):
        assert self.clean_df.shape[0] != 0, "Df is empty..."

    def test_df_smaller_rows(self):
        assert self.clean_df.shape[0] <= self.original_df[0], "Added extra rows!"

    def test_df_columns(self):
        expected_columns = ['inspection_id', 'dba_name', 'aka_name', 'license_', 'facility_type', 'risk', 'address',
                            'city', 'state', 'zip', 'inspection_date', 'inspection_type', 'results', 'latitude',
                            'longitude', 'location', 'violations']
        assert set(self.clean_df.columns).issubset(expected_columns), "Cleaning: ¿Qué columnas has agregado?"


    def runTest(self):
        print("Corriendo tests de limpieza de datos...")
        self.test_df_smaller_equal()
        self.test_df_not_empty()
        self.test_df_smaller_rows()
        self.test_df_columns()
        print(">>>> Tests de limpieza terminados <<<<")
