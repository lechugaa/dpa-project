#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 26 13:36:48 2021

@author: mario
"""
import os
import pickle

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_curve, roc_auc_score

class Modelling:
    
    prefix = 'modelling'
    
    def __init__(self, query_date, training = True):
        self.training = training
        self.query_date = query_date
    
        
        
    def _get_data(self):
        
        if self.training:
            date_string = self.query_date.strftime("%Y-%m-%d")
            root_path = os.getcwd()
            path = f"{root_path}-temp/feature-engineering-dataset-{date_string}-training.pkl"
        
            self.data_dict = pickle.load(open(path,'rb'))
            self.X_train = self.data_dict['X_train']
            # ... 
            
        else:
            date_string = self.query_date.strftime("%Y-%m-%d")
            root_path = os.getcwd()
            path = f"{root_path}-temp/feature-engineering-dataset-{date_string}-training.pkl"
        
            self.data_dict = pickle.load(open(path,'rb'))
            self.X_consec = self.data_dict['X_consec']
            self.y_consec = self.data_dict['y_consec']
            
            
        
    def _train_models(self):
    
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
        
        estimators_dict = {'tree': DecisionTreeClassifier(random_state=1111),
                   'random_forest': RandomForestClassifier(oob_score=True, random_state=2222)}
        
        algorithms = ['tree', 'random_forest']
        
        best_estimators = []
        
        for algorithm in algorithms:
            
            estimator = estimators_dict[algorithm]
            grid_search_to_look = algorithms_dict[algorithm]
            grid_params = grid_search_dict[grid_search_to_look]
        
            gs = GridSearchCV(estimator, grid_params, scoring='roc_auc', cv=5, n_jobs=-1)
        
            #train
            gs.fit(self.X_train, self.y_train)
            #best estimator
            best_estimators.append(gs)
            
        
        self.best_estimators = best_estimators
        
    def _compare_models(self, save = True):
            
            if self.training: 
                self.best_estimators
            
                self.y_test
            
                self.threshold = 0.65
            
                mod1 = evaluate(self.best_estimators[0].fit, self.y_test, threshold = self.threshold)
                #evaluate() considera hacer predicciones y compararlas con y_test
                mod2 = evaluate(self.best_estimators[1].fit, self.y_test, threshold = self.threshold)
            
                if mod1.auc > mod2.auc:
                    best_mod = mod1
                
                else: 
                    best_mod = mod2
            
            
                self.best_auc = best_mod.auc
                self.used_metric = ['auc']
                self.used_algo = best_mod.algorith
                self.used_hyperparams = best_mod.hyperparams
                self.model_def = best_mod
            
                if save:
                    pickle.dump(open(self.best_mod, 'temp/model_def.pkl'))
            
            else:
                print("Models are not trained when training = False")
            
        def _predict_labels(self):
             
             if self.training:
                 
                 self.model_def.predict(self.X_test)
              
             else:
                 
                 self.model = pickle.load(open("temp/saved_model_def.pkl"))
                 self.model_def.predict(self.X_consec)
        
        
        def train_model(self):
            
            self._get_data()
            self._train_models()
            self._compare_models()
            self._predict_labels()
              
            
        
        
            
            
        
      

        