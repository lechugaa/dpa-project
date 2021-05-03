import unittest


from src.utils.general import get_file_path, load_from_pickle
from src.pipeline.modelling import ModelSelector


class ModelSelectionTester(unittest.TestCase):
    def __init__(self, historic, query_date, desired_classes, *args, **kwargs):
        super(ModelSelectionTester, self).__init__(*args, **kwargs)
        self.historic = historic
        self.query_date = query_date
        self.desired_classes = desired_classes

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=ModelSelector.prefix, training=False)
        self.model = load_from_pickle(file_path)

    def test_classes(self):
        assert set(self.desired_classes) == set(self.model.classes_.tolist()), "Las clases del modelo no coinciden con las requeridas..."

    def runTest(self):
        print("Corriendo tests de selección de modelos...")
        self.test_classes()
        print(">>>> Tests de selección de modelos terminados <<<<")
