#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 26 06:01:00 2021

@author: cbautistap
"""
# Model Selection

import pickle
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
import time
from sklearn.metrics import roc_curve, roc_auc_score
import matplotlib.pyplot as plt


data_dict = pickle.load(open("temp/feature-engineering-dataset-2021-02-22-training.pkl",'rb'))

X_train = data_dict['X_train']
y_train = data_dict['y_train']
X_test = data_dict['X_test']
y_test = data_dict['y_test']

# Magic Loop

algorithms_dict = {'tree': 'tree_grid_search',
                  'random_forest': 'rf_grid_search'}

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


algorithms = ['tree','random_forest']
start_time = time.time()
best = magic_loop(algorithms, X_train, y_train)
#print("Tiempo en ejecutar: ", time.time() - start_time)


# La mejor configuración del Decission Tree es:
best_0 = best[0].best_estimator_
best_0
# La mejor configuración del Random Forest es:
best_1 = best[1].best_estimator_
best_1

# El score de Decission Tree es:
best[0].best_score_
# El score de Random Forest es:
best[1].best_score_


# Generamos predicciones con mejor modelo de RF
predicted_labels = best_1.predict(X_test)

# Calculamos scores con el mejor modelo de RF
predicted_scores = best_1.predict_proba(X_test)


# Generamos curva ROC y calculamos el AUC
fpr, tpr, thresholds = roc_curve(y_test, predicted_scores[:,1], pos_label=1)

plt.clf()
plt.plot([0,1],[0,1], 'k--', c="red")
plt.plot(fpr, tpr)
plt.title("ROC best RF, AUC: {}".format(roc_auc_score(y_test, predicted_labels)))
plt.xlabel("fpr")
plt.ylabel("tpr")
plt.show()

