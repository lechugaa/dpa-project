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
        file_path = get_file_path(
            historic=self.historic, query_date=self.query_date, prefix=DataCleaner.prefix)
        self.df = load_from_pickle(file_path)

        # getting S3 pickle
        self.uploaded_df = get_pickle_from_s3_to_pandas(
            historic=self.historic, query_date=self.query_date)

    def test_df_are_equal(self):
        pd.testing.assert_frame_equal(
            left=self.ingestion_df, right=self.uploaded_df)

    def runTest(self):
        print("Corriendo tests de almacenamiento...")
        self.test_df_are_equal()
        print("Tests de almacenamiento terminados...")
