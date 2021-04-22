import unittest
import pandas as pd
from src.utils.general import load_from_pickle


class IngestionTest(unittest.TestCase):

    def __init__(self, ingestion_path, *args, **kwargs):
        super(IngestionTest, self).__init__(*args, **kwargs)
        self.data_path = ingestion_path

    def setUp(self):
        ingestion_data = load_from_pickle(self.data_path)
        self.df = pd.DataFrame.from_dict(ingestion_data)

    def test_number_of_columns(self):
        assert self.df.shape[1] == 17, "Ingestión: El número de columnas no es 17"

    def test_df_not_empty(self):
        assert self.df.shape[0] != 0, "Ingestión: El df recibido está vacío"

    def test_df_columns(self):
        expected_columns = ['inspection_id', 'dba_name', 'aka_name','license_', 'facility_type', 'risk', 'address',
                            'city','state', 'zip', 'inspection_date', 'inspection_type', 'results', 'latitude',
                            'longitude', 'location', 'violations']
        assert list(self.df.columns) == expected_columns, "Ingestión: Las variables obtenidas no son las correctas"

    def runTest(self):
        print("Corriendo tests de ingesta...")
        self.test_number_of_columns()
        self.test_df_not_empty()
        self.test_df_columns()
        print("Tests de ingesta terminados...")
