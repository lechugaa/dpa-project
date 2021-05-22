import unittest
from src.pipeline.prediction import Predictor
from src.utils.general import get_file_path, load_from_pickle


class PredictionTester(unittest.TestCase):

    def __init__(self, query_date, min_desired_score, max_desired_score, *args, **kwargs):
        super(PredictionTester, self).__init__(*args, **kwargs)
        self.query_date = query_date
        self.min_desired_score = min_desired_score
        self.max_desired_score = max_desired_score

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=False, query_date=self.query_date, prefix=Predictor.prefix, training=False)
        self.df = load_from_pickle(file_path)

    def test_not_empty(self):
        assert self.df.shape[0] > 0, "Predictions dataframe is empty!"

    def test_labels(self):
        assert set(self.df.labels).issubset([0, 1]), "Predictions are not binary!"

    def test_min_score(self):
        assert min(self.df.score) >= self.min_desired_score, f"Minimum score is lower than {self.min_desired_score}!"

    def test_max_score(self):
        print()
        assert max(self.df.score) <= self.max_desired_score, f"Maximum score is higher than {self.max_desired_score}!"

    def runTest(self):
        print("Corriendo tests de predicción...")
        self.test_not_empty()
        self.test_labels()
        self.test_min_score()
        self.test_max_score()
        print(">>>> Tests de predicción terminados <<<<")
