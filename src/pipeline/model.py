#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 26 10:47:49 2021

@author: cbautistap
"""
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import time
import os
#import re

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_curve, roc_auc_score


from src.utils.general import get_pickle_from_s3_to_pandas, get_file_path
from src.utils.general import load_from_pickle, save_to_pickle


#data_dict = pickle.load(open("temp/feature-engineering-dataset-2021-02-22-training.pkl",'rb'))

#X_train = data_dict['X_train']
#y_train = data_dict['y_train']
#X_test = data_dict['X_test']
#y_test = data_dict['y_test']




# Algoritmos a evaluar: DecisionTree y RandomForest
algorithms_dict = {'tree': 'tree_grid_search',
                  'random_forest': 'rf_grid_search'}

# Hiperparámetros a evaluar en cada algoritmo:
grid_search_dict = {'tree_grid_search': {'max_depth': [5,10,15], 
                                         'min_samples_leaf': [3,5,7]},
                   'rf_grid_search': {'n_estimators': [300,400],  
                                      #'min_samples_leaf': [3,5],
                                      'max_depth':[7,10],
                                      'min_samples_split':[3],
                                      'max_features': [10,15,20],
                                      'criterion': ['gini']}}

# Configuraciones generales de cada algoritmo a evaluar:
estimators_dict = {'tree': DecisionTreeClassifier(random_state=1111),
                   'random_forest': RandomForestClassifier(oob_score=True, random_state=2222)}



#os.chdir(path + '/output')
data_dict = pickle.load(open("temp/feature-engineering-dataset-2021-02-22-training.pkl",'rb')) #leer el diccionario de la task anterior (feature-engineering)
X_train = data_dict['X_train'].head(500)
y_train = data_dict['y_train'].head(500)
X_test = data_dict['X_test']
y_test = data_dict['y_test']


best_estimators = []

algorithms = ['tree', 'random_forest']

for algorithm in algorithms:
    estimator = estimators_dict[algorithm]
    grid_search_to_look = algorithms_dict[algorithm]
    grid_params = grid_search_dict[grid_search_to_look]
        
    gs = GridSearchCV(estimator, grid_params, scoring='precision', cv=5, n_jobs=-1)
        
     #train
    gs.fit(X_train, y_train)
        #best estimator
    best_estimators.append(gs)


gs

best_estimators[0].fit
best_estimators[0].best_estimator_
best_estimators[0].best_score_








# PRIMERO QUIERO CARGAR LOS DATOS DEL PICKLE
# Aquí se puede hacer el método que carga el picke de la task anterior (como MARIO)

def load_features_labels(path):
    return X_train, y_train, X_test, y_test

def magic_loop(algorithms, features, labels):
    best_estimators = []
    for algorithm in algorithms:
        estimator = estimators_dict[algorithm]
        grid_search_to_look = algorithms_dict[algorithm]
        grid_params = grid_search_dict[grid_search_to_look]
        
        gs = GridSearchCV(estimator, grid_params, scoring='precision', cv=5, n_jobs=-1)
        
        #train
        gs.fit(features, labels)
        #best estimator
        best_estimators.append(gs)
        
        
    return best_estimators

def modeling(path):
    X_train_model, y_train = load_features(path)
    algorithms = ['tree', 'random_forest']
    models = magic_loop(algorithms, X_train_model, y_train)
    save_model(models,path)
    return


def modeling(path):
    """Función para cargar el pickle después de feature engineering.
    Obtiene los df spliteados.
    Entrena modelos con magic loop.
    Guarda modelos
    """
    X_train, y_train, X_test, y_test = load_features_labels(path)
    algorithms = ['tree', 'random_forest']
    models = magic_loop(algorithms, X_train, y_train)
    save_model(models,path)
    return









    def _get_df(self):
        """Función para cargar el pickle que se guarda en local
        de la task anterior.
        """
        pickle_task_anterior = get_file_path(
            self.historic, self.query_date, prefix='')
        df = load_from_pickle(pickle_task_anterior)      
        df.violations = df.violations.astype('str')
        return df


class Modeling:
    
    # static variables
    prefix = 'modelling'
    
    def __init__(self,
    pass

















class DataCleaner:

    # static variables
    prefix = 'clean'

    def __init__(self, historic=False, query_date=None):
        self.df = get_pickle_from_s3_to_pandas(historic, query_date)
        #Parche
        #self.df = pd.DataFrame(pd.read_pickle('temp/historic-inspections-2021-02-22.pkl'))
        # Parche
        self.historic = historic
        self.query_date = query_date
        self.prefix = DataCleaner.prefix

    def _subset_cols(self):
        self.df = self.df[['inspection_id', 'facility_type', 'risk', 'zip', 'inspection_date',
                           'inspection_type', 'results', 'violations', 'latitude', 'longitude']]

    def _fill_nas(self):
        self.df['zip'] = self.df['zip'].fillna(0)
        self.df[['facility_type', 'inspection_type', 'violations']] = self.df[[
            'facility_type', 'inspection_type', 'violations']].fillna('na')

    def _change_data_types(self):
        self.df = self.df.astype({
            'inspection_date': 'datetime64',
            'longitude': 'double',
            'latitude': 'double',
            'zip': 'int'})
        self.df = self.df.astype({"zip": 'str'})

    def _clean_results(self):
        self.df['results'].mask(self.df['results'] !=
                                'Pass', other='Not Pass', inplace=True)

    def _standardize_column_strings(self, columns, excluded_punctuation=".,-*'¿?¡!()", gap_punct="\/"):
        for col in columns:
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace(" ", "_"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("á", "a"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("é", "e"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("í", "i"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("ó", "o"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("ú", "u"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("ü", "u"))

            for ch in excluded_punctuation:
                self.df[col] = self.df[col].apply(lambda x: x.replace(ch, ""))

            for ch in gap_punct:
                self.df[col] = self.df[col].apply(lambda x: x.replace(ch, "_"))

    def _drop_risk_all(self):
        self.df = self.df[self.df['risk'] != 'all']
        
    def _clean_facility_type(self):

        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('restuarant', 'restaurant'))
        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('theatre', 'theater'))
        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('herabal', 'herbal'))
        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('day_care', 'daycare'))
        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('long_term', 'longterm'))

        self.df.loc[self.df['facility_type'].str.contains(
            'childern|children|1023|5_years_old', case=False, na=None), 'facility_type'] = 'childrens_service_facility'
        self.df.loc[self.df['facility_type'].str.contains(
            'conv|mart|gas_station_store', case=False, na=None), 'facility_type'] = 'convenience_store'
        self.df.loc[self.df['facility_type'].str.contains(
            'assis|longterm|nursing|supportive', case=False, na=None), 'facility_type'] = 'assisted_living'
        self.df.loc[self.df['facility_type'].str.contains(
            'herbal_life|herbalife|herbalcal', case=False, na=None), 'facility_type'] = 'herbalife'
        self.df.loc[self.df['facility_type'].str.contains(
            'after_school', case=False, na=None), 'facility_type'] = 'after_school'
        self.df.loc[self.df['facility_type'].str.contains(
            'tavern|pub|brew|wine_tasting|bar_grill|hooka', case=False, na=None), 'facility_type'] = 'bar'
        self.df.loc[self.df['facility_type'].str.contains(
            'bakery', case=False, na=None), 'facility_type'] = 'bakery'
        self.df.loc[self.df['facility_type'].str.contains(
            'mobil|truck|mfd', case=False, na=None), 'facility_type'] = 'mobile_food'
        self.df.loc[self.df['facility_type'].str.contains(
            'kitchen', case=False, na=None), 'facility_type'] = 'kitchen'
        self.df.loc[self.df['facility_type'].str.contains(
            'restaurant|rstaurant|diner', case=False, na=None), 'facility_type'] = 'restaurant'
        self.df.loc[self.df['facility_type'].str.contains(
            'retail', case=False, na=None), 'facility_type'] = 'retail'
        self.df.loc[self.df['facility_type'].str.contains(
            'roof', case=False, na=None), 'facility_type'] = 'rooftop'
        self.df.loc[self.df['facility_type'].str.contains(
            'grocery', case=False, na=None), 'facility_type'] = 'grocery_store'
        self.df.loc[self.df['facility_type'].str.contains(
            'liquor', case=False, na=None), 'facility_type'] = 'liquor'
        self.df.loc[self.df['facility_type'].str.contains(
            'popup', case=False, na=None), 'facility_type'] = 'popup_establishment'
        self.df.loc[self.df['facility_type'].str.contains(
            'school|college|shcool', case=False, na=None), 'facility_type'] = 'school'
        self.df.loc[self.df['facility_type'].str.contains(
            'daycare', case=False, na=None), 'facility_type'] = 'daycare'
        self.df.loc[self.df['facility_type'].str.contains(
            'cafeteria|coffee|cafe', case=False, na=None), 'facility_type'] = 'coffee'
        self.df.loc[self.df['facility_type'].str.contains(
            'drug_store|pharmacy', case=False, na=None), 'facility_type'] = 'drug_store'
        self.df.loc[self.df['facility_type'].str.contains(
            'gym|fitness|weight_loss|exercise', case=False, na=None), 'facility_type'] = 'gym'
        self.df.loc[self.df['facility_type'].str.contains(
            'commissary|machine|commiasary', case=False, na=None), 'facility_type'] = 'vending_machine'
        self.df.loc[self.df['facility_type'].str.contains(
            'ice_cream|paleteria|gelato', case=False, na=None), 'facility_type'] = 'ice_cream'
        self.df.loc[self.df['facility_type'].str.contains(
            'banquet', case=False, na=None), 'facility_type'] = 'banquet'
        self.df.loc[self.df['facility_type'].str.contains(
            'lounge', case=False, na=None), 'facility_type'] = 'lounge'
        self.df.loc[self.df['facility_type'].str.contains(
            'church|religious', case=False, na=None), 'facility_type'] = 'church'
        self.df.loc[self.df['facility_type'].str.contains(
            'kiosk', case=False, na=None), 'facility_type'] = 'kiosk'
        self.df.loc[self.df['facility_type'].str.contains(
            'health|rehab', case=False, na=None), 'facility_type'] = 'health'
        self.df.loc[self.df['facility_type'].str.contains(
            'event', case=False, na=None), 'facility_type'] = 'events'
        self.df.loc[self.df['facility_type'].str.contains(
            'donut|hotdog|hot_dog|popcorn|juice|tea|dessert|deli|salad|snack|candy|shake|watermelon|smoothie|food|sushi', case=False, na=None), 'facility_type'] = 'other_food'
        self.df.loc[self.df['facility_type'].str.contains(
            'poultry|butcher|slaughter|meat', case=False, na=None), 'facility_type'] = 'butcher'
        self.df.loc[self.df['facility_type'].str.contains(
            'profit', case=False, na=None), 'facility_type'] = 'non_profit'
        # self.df.loc[self.df['facility_type'].str.contains('na', case=False, na=None), 'facility_type'] = 'not_specified'

    def _clean_inspection_type(self):
        self.df.loc[self.df['inspection_type'].str.contains(
            'license', case=False, na=None), 'inspection_type'] = 'license'
        self.df.loc[self.df['inspection_type'].str.contains(
            'task_force|taskforce', case=False, na=None), 'inspection_type'] = 'task_force'
        self.df.loc[self.df['inspection_type'].str.contains(
            'canvass|canvas', case=False, na=None), 'inspection_type'] = 'canvas'
        self.df.loc[self.df['inspection_type'].str.contains(
            'complaint', case=False, na=None), 'inspection_type'] = 'complaint'
        self.df.loc[self.df['inspection_type'].str.contains(
            'food|sick', case=False, na=None), 'inspection_type'] = 'suspected_food_poisoning'

    def _crea_num_violations(self):
        self.df['num_violations'] = self.df['violations'].apply(
            lambda x: x.count(' | ') + 1 if x != 'na' else 0)
        
    def _sort_by_date(self):
        self.df.sort_values('inspection_date', inplace = True)
        self.df.reset_index(inplace = True,drop = True)

    def clean_data(self, save=False):
        print("Cleaning records..")
        self.original_rows, self.original_cols = self.df.shape
        # Codigo Agregado MH
        self._subset_cols()
        self._fill_nas()
        self.df.dropna(axis = 0, inplace = True)
        self._change_data_types()
        self._clean_results()
        self._standardize_column_strings(['facility_type', 'risk', 'inspection_type', 'results'])
        self._drop_risk_all()
        self._clean_facility_type()
        self._clean_inspection_type()
        self._crea_num_violations()
        self._sort_by_date()
        ###
        self.final_rows, self.final_cols = self.df.shape
        print("Records are clean and ready to be uploaded")
        if save:
            self._save_df()

    def _save_df(self):
        local_path = get_file_path(self.historic, self.query_date, self.prefix)
        pickle.dump(self.df, open(local_path, 'wb'))
        print(f"Successfully saved temp file as pickle in: {local_path}")

    def get_clean_df(self):
        self.clean_data()
        return self.df

    def get_cleaning_metadata(self):
        if self.query_date is None:
            self.query_date = datetime.datetime.now()

        return [(self.original_rows,
                 self.original_cols,
                 self.final_rows,
                 self.final_cols, self.historic, self.query_date)]
    
    
    
    
    
    






