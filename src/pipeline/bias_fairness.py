from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
import datetime
import os
import pandas as pd
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
        self.predictions = self.model.predict(self.features)  # predicciones

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
        print(f"***\nSelected model in previous task: {self.model} ***")
        # nota: aquí es cuando encuentro confuso el parámetro  training: los modelos no lo tienen
        # y por eso no lo pueden heredar de la clase

    def _construct_aequitas_frame(self):
        """
        Dado que ya se hizo el one hot encoding, esto es un ligero dolor.
        """
        facility_types = [e for e in self.features.columns if e.startswith('facility')]
        chosen_facilities = [0] * self.features.shape[0]
        for i in range(len(self.features)):
            for facility_type in facility_types:
                if self.features[facility_type][i] == 1:
                    chosen_facilities[i] = facility_type
                    break

        self.aequitas_df = pd.DataFrame({'score': self.predictions, 'label_value': self.labels,
                                         'facility_type': chosen_facilities})
        print(f"***\nSuccessfully constructed aequitas dataframe with columns: {self.aequitas_df.columns.values}***")