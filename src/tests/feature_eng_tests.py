import unittest
import pandas as pd

from src.pipeline.limpieza_feature_eng import DataEngineer
from src.utils.general import load_from_pickle, get_file_path, get_pickle_from_s3_to_pandas, load_from_pickle


class FeatureTester(unittest.TestCase):
   
    def __init__(self, historic, query_date, *args, **kwargs):
        super(FeatureTester, self).__init__(*args, **kwargs)
        self.historic = historic
        self.query_date = query_date

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=DataEngineer.prefix)
        self.df = load_from_pickle(file_path)

    def test_df_not_empty(self):
        # fue el único test  que agregué porque en teoría feature engineering debería
        # permitir tanto quitar como poner columnas; o incluso renglones con oversampling

        assert self.df.shape[0] != 0, "Df is empty..."


    def runTest(self):
        print("Corriendo tests de limpieza de datos...")
        self.test_df_not_empty()
        print(">>>> Tests de limpieza terminados <<<<")
