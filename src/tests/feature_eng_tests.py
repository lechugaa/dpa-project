import unittest

from src.pipeline.limpieza_feature_eng import DataEngineer
from src.utils.general import get_file_path, load_from_pickle


class FeatureTester(unittest.TestCase):
   
    def __init__(self, historic, query_date, training, *args, **kwargs):
        super(FeatureTester, self).__init__(*args, **kwargs)
        self.historic = historic
        self.query_date = query_date
        self.training = training

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=DataEngineer.prefix, training=self.training)
        fe_dataset = load_from_pickle(file_path)

        if self.training:
            self.x_train = fe_dataset['X_train']
            self.y_train = fe_dataset['y_train']
            self.x_test = fe_dataset['X_test']
            self.y_test = fe_dataset['y_test']
        else:
            self.X = fe_dataset['X_consec']
            self.y = fe_dataset['y_consec']

    def test_df_not_empty(self):
        # fue el único test  que agregué porque en teoría feature engineering debería permitir tanto quitar como
        # poner columnas; o incluso renglones
        if self.training:
            assert self.x_train.shape[0] != 0, "x_train df is empty"
            assert self.y_train.shape[0] != 0, "y_train df is empty"
            assert self.x_test.shape[0] != 0, "x_test df is empty"
            assert self.y_test.shape[0] != 0, "y_test df is empty"
        else:
            assert self.X.shape[0] != 0, "x_pred df is empty"
            assert self.y.shape[0] != 0, "y_pred df is empty"

    def runTest(self):
        print("Corriendo tests de limpieza de datos...")
        self.test_df_not_empty()
        print(">>>> Tests de limpieza terminados <<<<")
