import unittest
from src.pipeline.bias_fairness import MrFairness
from src.utils.general import get_file_path, load_from_pickle


class AequitasTester(unittest.TestCase):

    def __init__(self, historic, query_date, min_categories=2, *args, **kwargs):
        super(AequitasTester, self).__init__(*args, **kwargs)
        self.historic = historic
        self.query_date = query_date
        self.min_categories = min_categories

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=self.historic, query_date=self.query_date,
                                  prefix=MrFairness.prefix, training=True)
        data = load_from_pickle(file_path)
        self.group = data['group']
        self.bias = data['bias']
        self.fairness = data['fairness']

    def test_df_not_empty(self):
        assert self.group['counts'].shape[0] != 0, "Group counts is empty"
        assert self.group['percent'].shape[0] != 0, "Group percentage is empty"
        assert self.bias['dataframe'].shape[0] != 0, "Bias df is empty"
        assert self.bias['summary'].shape[0] != 0, "Bias small df is empty"
        assert self.fairness['group'].shape[0] != 0, "Fairness by group df is empty"
        assert self.fairness['attributes'].shape[0] != 0, "Fairness by attributes df is empty"
        # assert self.fairness['overall'].shape[0] != 0, "Overall fairness is empty"

    def test_number_of_categories(self):
        assert self.group['percent'].shape[0] >= self.min_categories, f"The number of categories is lower than {self.min_categories}"

    def runTest(self):
        print("Corriendo tests de aequitas...")
        self.test_df_not_empty()
        self.test_number_of_categories()
        print(">>>> Tests de aequitas terminados <<<<")
