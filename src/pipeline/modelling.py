import pickle

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

from src.utils.general import get_dictionary_from_s3, get_file_path


class Modelling:
    prefix = 'modelling'
    algorithms = ['tree', 'random_forest']
    algorithms_dict = {'tree': 'tree_grid_search', 'random_forest': 'rf_grid_search'}
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
        data_dict = get_dictionary_from_s3(historic=self.historic, query_date=self.query_date, training=True)
        self.x_train = data_dict['X_train']
        self.y_train = data_dict['y_train']
        self.x_test = data_dict['X_test']
        self.y_test = data_dict['y_test']

    def _magic_loop(self):
        self.best_estimators = []
        for algorithm in Modelling.algorithms:
            estimator = Modelling.estimators_dict[algorithm]
            grid_search_to_look = Modelling.algorithms_dict[algorithm]
            grid_params = Modelling.grid_search_dict[grid_search_to_look]
            gs = GridSearchCV(estimator, grid_params, scoring='roc_auc', cv=5, n_jobs=-1)

            # train
            gs.fit(self.x_train, self.y_train)
            self.best_estimators.append(gs.best_estimator_)

    def _save_models(self):
        local_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=self.prefix)
        pickle.dump(self.best_estimators, open(local_path, 'wb'))
        print(f"Successfully saved models as pickle in: {local_path}")

    def get_models(self):
        return self.best_estimators

    # def _seleccionar_modelo(self, save = True):
    #         # AQUI METER TODO LO DE AUC DE CV (ver notebook)
    #         # LUEGO METER TODO LO DE AUC Y PREDICCIONES CON BEST TREE Y BEST RF AQUI SE DEFINE EL MEJOR MODELO
    #         # LUEGO
    #         if self.training:
    #             self.best_estimators
    #
    #             self.y_test
    #
    #             self.threshold = 0.65
    #
    #             mod1 = evaluate(self.best_estimators[0].fit, self.y_test, threshold = self.threshold)
    #             #evaluate() considera hacer predicciones y compararlas con y_test
    #             mod2 = evaluate(self.best_estimators[1].fit, self.y_test, threshold = self.threshold)
    #
    #             if mod1.auc > mod2.auc:
    #                 best_mod = mod1
    #
    #             else:
    #                 best_mod = mod2
    #
    #
    #             self.best_auc = best_mod.auc
    #             self.used_metric = ['auc']
    #             self.used_algo = best_mod.algorith
    #             self.used_hyperparams = best_mod.hyperparams
    #             self.model_def = best_mod
    #
    #             if save:
    #                 pickle.dump(open(self.best_mod, 'temp/model_def.pkl'))
    #
    #         else:
    #             print("Models are not trained when training = False")
    #
    #
    # def _entrenar_modelo_def(self):
    #
    #
    # def _predict_labels(self):
    #
    #     if self.training:
    #
    #         self.model_def.predict(self.X_test)
    #
    #     else:
    #
    #         self.model = pickle.load(open("temp/saved_model_def.pkl"))
    #         self.model_def.predict(self.X_consec)
    #
    #
    #     def train_model(self):
    #
    #         self._get_data()
    #         self._train_models()
    #         self._compare_models()
    #         self._predict_labels()
