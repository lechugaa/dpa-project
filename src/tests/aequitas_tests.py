import unittest
from src.pipeline.bias_fairness import MrFairness
from src.utils.general import get_file_path, load_from_pickle


class AequitasTester(unittest.TestCase):

    def __init__(self, historic, query_date, training, *args, **kwargs):
        super(AequitasTester, self).__init__(*args, **kwargs)
        self.historic = historic
        self.query_date = query_date
        self.training = training

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=self.historic, query_date=self.query_date,
                                  prefix=DataEngineer.prefix, training=self.training)
        data = load_from_pickle(file_path)

        if self.training:
            self.group = data['group']
            self.bias = data['bias']
            self.fairness = data['fairness']

    def test_df_not_empty(self):
        # fue el único test  que agregué porque en teoría feature engineering debería permitir tanto quitar como
        # poner columnas; o incluso renglones
        if self.training:
            assert self.group['counts'].shape[0] != 0, "Group counts is empty"
            assert self.group['percentage'].shape[0] != 0, "Group percentage is empty"
            assert self.bias['dataframe'].shape[0] != 0, "Bias df is empty"
            assert self.bias['summary'].shape[0] != 0, "Bias small df is empty"
            assert self.fairness['group'].shape[0] != 0, "Fairness by group df is empty"
            assert self.fairness['attributes'].shape[0] != 0, "Fairness by attributes df is empty"
            assert self.fairness['overall'].shape[0] != 0, "Overall fairness is empty"
            
        else:
            assert 100 == 100, "We are not in training so it doesn't make sense to test this."

    def runTest(self):
        print("Corriendo tests de limpieza de datos...")
        self.test_df_not_empty()
        print(">>>> Tests de limpieza terminados <<<<")
