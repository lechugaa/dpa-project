from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
import datetime
import os
import pandas
import pickle

from src.pipeline.limpieza_feature_eng import DataEngineer
from src.pipeline.modelling import ModelSelector
from src.utils.general import get_file_path, load_from_pickle, save_to_pickle, get_upload_path
from src.utils.general import get_object_from_s3


class MrFairness:

    prefix = 'bias-fairness'
    training = True

    def __init__(self, historic=False, query_date=None, training=True):
        self.historic = historic
        self.query_date = query_date
        self.training = training
        self.prefix = MrFairness.prefix
        self._load_data()
        self._load_model()

    def _load_data(self):
        """
        Función para cargar el pickle que se guarda en local
        de la task anterior y guardar como variables X_test y y_test.
        """
        # go for it
        pickle_task_anterior = get_file_path(
            self.historic, self.query_date, prefix=DataEngineer.prefix, training=self.training)
        feature_eng_dict = load_from_pickle(pickle_task_anterior)

        self.features = feature_eng_dict['X_test']
        self.labels = feature_eng_dict['y_test']
        print(f""" *** Successfully loaded features and labels from previous task. ***""")
        print(
            f"""\nFeatures dataframe has {self.features.shape[0]} rows and {self.features.shape[1]} columns.""")

    def _load_model(self):
        """
        Realmente Aequitas necesita comparar las etiquetas reales con las predichas,
        por lo que necesitamos cargar el modelo elegido como el mejor.
        """
        self.model = get_object_from_s3(historic=self.historic, query_date=self.query_date,
                                        prefix=ModelSelector.prefix, training=False)
        print(f"*** Selected model in previous task: {self.model} ***")
        # nota: aquí es cuando encuentro confuso el parámetro  training: los modelos no lo tienen
        # y por eso no lo pueden heredar de la clase
