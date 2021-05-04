from datetime import datetime
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

ATTRIBUTES_COLS = ['attribute_name', 'attribute_value']


class MrFairness:

    prefix = 'aequitas'

    def __init__(self, historic=False, query_date=None, training=True, save=False):
        self.historic = historic
        self.query_date = query_date
        self.training = training
        self.prefix = MrFairness.prefix
        self.alpha_bias = 0.05  # se usa en la sección de bias
        self.protected_group = 'facility_type_restaurant'
        self._load_data()
        self._load_model()
        self.predictions = self.model.predict(self.features)  # predicciones
        self.save = save
        self._execute_aequitas_pipeline()

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
        print(f"\n*** Successfully loaded model {self.model} ***")
        # nota: aquí es cuando encuentro confuso el parámetro  training: los modelos no lo tienen
        # y por eso no lo pueden heredar de la clase

    def _construct_aequitas_frame(self):
        """
        Dado que ya se hizo el one hot encoding, esto es un ligero dolor.
        """
        facility_types = [
            e for e in self.features.columns if e.startswith('facility')]
        chosen_facilities = [0] * self.features.shape[0]
        for i in range(len(self.features)):
            for facility_type in facility_types:
                if self.features[facility_type][i] == 1:
                    chosen_facilities[i] = facility_type
                    break

        self.aequitas_df = pd.DataFrame({'score': self.predictions, 'label_value': self.labels,
                                         'facility_type': chosen_facilities})
        print(
            f"\n>>> Successfully constructed aequitas dataframe with columns: {self.aequitas_df.columns.values} <<<")

    def _compute_group_metrics(self):
        """
        Método para calcular y guardar las métricas iniciales de grupo, tanto
        absolutas como relativas. Los resultados se guardan como dataframe.
        """
        group = Group()
        self.all_metrics_df, self.attributes = group.get_crosstabs(
            self.aequitas_df)
        self.absolute_metrics = group.list_absolute_metrics(
            self.all_metrics_df)
        self._construct_group_dataframes()

    def _construct_group_dataframes(self):
        """
        Función para construir los dos dataframes que muestra liliana en su
        notebook: uno para conteos absolutos y otro para los relativos.
        """
        # Primero: conteos absolutos
        columns = [
            col for col in self.all_metrics_df if col not in self.absolute_metrics]
        self.group_counts_df = self.all_metrics_df[columns]

        # Luego: conteos como porcentaje
        columns = ATTRIBUTES_COLS + self.absolute_metrics
        self.group_pct_df = self.all_metrics_df[columns].round(2)
        print("\nSuccessfully constructed Group dataframes: 'group_counts_df' and 'group_pct_df'")

    def _compute_bias_metrics(self):
        """
        Método para calcular las métricas relevantes para la sección de sesgo, o Bias,
        y guardarlas en dos dataframes. 'bias_df' es un subset de 'extended_bias_df',
        el cual contiene todas las columnas posibles.
        """
        self.bias = Bias()
        # 46 columnas
        self.extended_bias_df = self.bias.get_disparity_predefined_groups(self.all_metrics_df,
                                                                          original_df=self.aequitas_df, ref_groups_dict={
                                                                              'facility_type': self.protected_group},
                                                                          alpha=self.alpha_bias)

        self.bias_metrics = self.bias.list_disparities(self.extended_bias_df)
        important_columns = ATTRIBUTES_COLS + self.bias_metrics
        self.bias_df = self.extended_bias_df[important_columns].round(2)
        print(
            "\nSuccessfully constructed Bias dataframes: 'extended_bias_df' and 'bias_df'")

    def _compute_fairness_metrics(self):
        """
        Método para calcular y almacenar las métricas relevantes de fairness.
        """
        self.fairness = Fairness()
        self.fairness_df = self.fairness.get_group_value_fairness(
            self.extended_bias_df)
        self.parity_determinations = self.fairness.list_parities(
            self.fairness_df)
        self._construct_fairness_dataframes()

    def _construct_fairness_dataframes(self):
        """
        En esta función se crean y se almacenan los tres tipos de fairness analyses
        posibles.
        """
        # Primero: Equidad por grupo
        cols = ATTRIBUTES_COLS + self.absolute_metrics + self.bias.list_disparities(
            self.fairness_df) + self.parity_determinations
        self.fairness_by_group = self.fairness_df[cols].round(2)

        # Segundo: Equidad a nivel de atributo
        self.fairness_by_atts = self.fairness.get_group_attribute_fairness(
            self.fairness_df)

        # Tercero: overall fairness
        self.overall_fairness = self.fairness.get_overall_fairness(
            self.fairness_df)

        print("\nSuccessfully constructed Fairness dataframes: 'fairness_by_group', 'fairness_by_atts' and 'overall_fairness'")

    def _execute_aequitas_pipeline(self):
        self._construct_aequitas_frame()
        self._compute_group_metrics()
        self._compute_bias_metrics()
        self._compute_fairness_metrics()
        if self.save:
            self._save_data()
    
    def _save_data(self):
        file_path = get_file_path(historic=self.historic, query_date=self.query_date,
                                  prefix=MrFairness.prefix, training=self.training)
        pickle.dump(self, open(file_path, 'wb'))
        print(f"Successfully saved MrFairness object in {file_path}")


"""
## pruebas EC2:
from src.pipeline.bias_fairness import MrFairness
from datetime import datetime
date = datetime(2021, 4, 30)
fair = MrFairness(historic=False, query_date=date, training=True)
##
"""
