import unittest


from src.utils.general import get_file_path, load_from_pickle
from src.pipeline.modelling import Modelling


class TrainingTester(unittest.TestCase):
    def __init__(self, historic, query_date, desired_models, *args, **kwargs):
        super(TrainingTester, self).__init__(*args, **kwargs)
        self.historic = historic
        self.query_date = query_date
        self.desired_models = desired_models

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=Modelling.prefix, training=False)
        self.models = load_from_pickle(file_path)

    def test_enough_models(self):
        assert self.desired_models == len(self.models), "No hay suficientes modelos entrenados..."

    def runTest(self):
        print("Corriendo tests de entrenamiento...")
        self.test_enough_models()
        print(">>>> Tests de entrenamiento terminados <<<<")
