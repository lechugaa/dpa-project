import os
import pickle
import time
import numpy as np
import pandas as pd

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_auc_score, roc_curve, precision_recall_curve

from src.utils.general import get_object_from_s3, get_file_path, load_from_pickle, save_to_pickle


class Modelling:
    prefix = 'modelling'
    algorithms = ['tree', 'random_forest']
    algorithms_dict = {'tree': 'tree_grid_search', 'random_forest': 'rf_grid_search'}
    split_criteria = 'gini'
    grid_search_dict = {'tree_grid_search': {'max_depth': [5, 10, 15], 'min_samples_leaf': [3, 5, 7]},
                        'rf_grid_search': {'n_estimators': [300, 400], 'max_depth': [7, 10],
                                           'min_samples_split': [3], 'max_features': [10, 15, 20],
                                           'criterion': ['gini']}}
    estimators_dict = {'tree': DecisionTreeClassifier(random_state=1111),
                       'random_forest': RandomForestClassifier(oob_score=True, random_state=2222)}
    
    def __init__(self, historic, query_date, save_models=False):
        self.historic = historic
        self.query_date = query_date
        self.prefix = Modelling.prefix
        self.save_models = save_models
        self._get_data()
        self._magic_loop()
        if self.save_models:
            self._save_models()

    def _get_data(self):
        data_dict = get_object_from_s3(historic=self.historic, query_date=self.query_date, training=True)
        self.x_train = data_dict['X_train']
        self.y_train = data_dict['y_train']
        self.x_test = data_dict['X_test']
        self.y_test = data_dict['y_test']

    def _magic_loop(self):
        start_time = time.time()
        self.best_estimators = []

        for algorithm in Modelling.algorithms:
            estimator = Modelling.estimators_dict[algorithm]
            grid_search_to_look = Modelling.algorithms_dict[algorithm]
            grid_params = Modelling.grid_search_dict[grid_search_to_look]
            gs = GridSearchCV(estimator, grid_params, scoring='roc_auc', cv=5, n_jobs=-1)

            # train
            gs.fit(self.x_train, self.y_train)
            self.best_estimators.append(gs.best_estimator_)

        self.training_time = time.time() - start_time

    def _save_models(self):
        local_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=self.prefix)
        pickle.dump(self.best_estimators, open(local_path, 'wb'))
        print(f"Successfully saved models as pickle in: {local_path}")

    def get_models(self):
        return self.best_estimators

    def get_modeling_metadata(self):
        no_of_model = len(Modelling.algorithms)
        algorithms = '-'.join(self.algorithms)
        return [(self.historic, self.query_date, no_of_model, algorithms, self.training_time, self.split_criteria)]


class ModelSelector:

    prefix = "model-selection"

    def __init__(self, historic, query_date, fpr_restriction=0.05, save_model=False):
        self.historic = historic
        self.query_date = query_date
        self.prefix = ModelSelector.prefix
        self.fpr_restriction = fpr_restriction
        self.save_model = save_model

        # llamadas de métodos
        self._get_trained_models()
        self._get_data()
        self._evaluate_models()
        self._get_cutting_threshold()
        self._save_cutting_threshold()
        if self.save_model:
            self._save_model()
            self._save_predictions()
        self.get_selection_metadata()

    def _get_trained_models(self):
        file_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=Modelling.prefix, training=False)
        self.models = load_from_pickle(file_path)

    def _get_data(self):
        data_dict = get_object_from_s3(historic=self.historic, query_date=self.query_date, training=True)
        self.x_train = data_dict['X_train']
        self.y_train = data_dict['y_train']
        self.x_test = data_dict['X_test']
        self.y_test = data_dict['y_test']

    def _evaluate_models(self):
        best_auc = -1
        best_model = None
        best_label_scores = None
        best_fpr = None
        best_tpr = None
        best_thresholds = None

        for model in self.models:
            model.fit(self.x_train, self.y_train)
            # TODO: histograma de estas probas
            label_scores = model.predict_proba(self.x_test)
            auc = roc_auc_score(self.y_test, label_scores[:, 1])
            fpr, tpr, thresholds = roc_curve(self.y_test, label_scores[:, 1], pos_label=1)

            if auc > best_auc:
                best_auc = auc
                best_model = model
                best_label_scores = label_scores
                best_fpr = fpr
                best_tpr = tpr
                best_thresholds = thresholds

        self.best_auc = best_auc
        self.best_model = best_model
        self.label_scores = best_label_scores
        self.fpr = best_fpr
        self.tpr = best_tpr
        self.thresholds = best_thresholds

    def _save_model(self):
        local_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=self.prefix)
        pickle.dump(self.best_model, open(local_path, 'wb'))
        print(f"Successfully saved best model as pickle in: {local_path}")

    def _get_metrics_report(self, precision, recall, thresholds_2):
        df_1 = pd.DataFrame({'threshold': thresholds_2, 'precision': precision, 'recall': recall})
        df_1['f1_score'] = 2 * (df_1.precision * df_1.recall) / (df_1.precision + df_1.recall)

        df_2 = pd.DataFrame({'tpr': self.tpr, 'fpr': self.fpr, 'threshold': self.thresholds})
        df_2['tnr'] = 1 - df_2['fpr']
        df_2['fnr'] = 1 - df_2['tpr']

        return df_1.merge(df_2, on="threshold")

    def _get_cutting_threshold(self):
        precision, recall, thresholds_2 = precision_recall_curve(self.y_test, self.label_scores[:, 1], pos_label=1)
        thresholds_2 = np.append(thresholds_2, 1)
        metrics_report = self._get_metrics_report(precision, recall, thresholds_2)
        negocio = metrics_report[metrics_report.fpr <= self.fpr_restriction]
        self.cutting_threshold = negocio.head(1).threshold.values[0]

    def _save_cutting_threshold(self):
        root_path = os.getcwd()
        cutting_info = {'cutting_threshold': self.cutting_threshold}
        path = f"{root_path}/temp/cutting_info.pkl"
        save_to_pickle(cutting_info, path)
        
    def _save_predictions(self):
        root_path = os.getcwd()
        preds = self.label_scores
        path = f"{root_path}/temp/{self.prefix}-predicted-scores.pkl"
        save_to_pickle(preds, path)
        
    def get_selection_metadata(self):
        best_model_desc = str(self.best_model)
        possible_models = len(self.models)

        metadata = [(self.historic,
                     self.query_date,
                     best_model_desc,
                     possible_models,
                     self.cutting_threshold,
                     self.fpr_restriction,
                     self.best_auc)]

        return metadata
