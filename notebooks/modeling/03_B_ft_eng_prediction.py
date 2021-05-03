#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 24 13:47:35 2021

@author: mario
"""
import pickle
import pandas as pd
from datetime import datetime
from src.pipeline.limpieza_feature_eng import DataCleaner
from src.pipeline.limpieza_feature_eng import DataEngineer

## Definir Funcion Step Other

def step_other(df_train, df_test, cols, thresh = 0.002, others_dict = None):
    
    if others_dict is None:
    
        others_dict = {}
        df_train_copy = df_train.copy()
    
        df_test_copy = df_test.copy()
    
        for col in cols:
            # Obtener los niveles que pasan el thresh con train
            lvls = df_train[col].value_counts(normalize=True)[df_train[col].value_counts(normalize=True) > thresh].index.array
            # Almacenar los niveles en un dict
            others_dict[col] = lvls
            # Hacer el cambio a 'other' en el df_train
            df_train_copy[col] = df_train[col].mask(~df_train[col].isin(lvls), 'other')
            # Hacer el  cambio a 'other'en el df_test
            df_test_copy[col] = df_test[col].mask(~df_test[col].isin(lvls), 'other')
            
        return df_train_copy, df_test_copy, others_dict

    else:
        df_test_copy = df_test.copy()
        
        for col in cols:
            lvls = others_dict[col]
            df_test_copy[col] = df_test[col].mask(~df_test[col].isin(lvls), 'other')
        
        return df_test_copy    
            

#Importar data

d = DataCleaner(historic= False, query_date=datetime.strptime('2021-03-08', '%Y-%m-%d'))
clean_dset = d.get_clean_df()

clean_dset

#Entrenar transformers con historic = True, training = True
e = DataEngineer(historic= True, query_date=datetime.strptime('2021-02-22', '%Y-%m-%d'), training= True)
other_dict, scaler, encoder = e.get_trained_preprocess()

y = clean_dset['results'] 
X = clean_dset.drop(columns = 'results')


# Extraer atributos de fechas

X['dow'] = X['inspection_date'].dt.day_name().str.lower()
X['month'] = X['inspection_date'].dt.month
X = X.astype({"month": 'str'})


#Step other
X = step_other(None , df_test = X, cols = ['inspection_type', 'facility_type','zip'], others_dict= other_dict)

#Escalar lat y long

X[['longitude','latitude']] = scaler.transform(X[['longitude','latitude']])

#One hot encoding

onehot_test = pd.DataFrame(encoder.transform(X[['inspection_type', 'facility_type', 'zip', 'risk','dow','month']]))
onehot_test.columns = encoder.get_feature_names(['inspection_type', 'facility_type', 'zip', 'risk','dow','month'])
onehot_test

X.drop(columns = ['inspection_type', 'facility_type', 'zip', 'risk','dow','month'], inplace= True)

X = pd.concat([X, onehot_test], axis = 1)

#Drop useless columns

X.drop(columns = ['inspection_id', 'inspection_date', 'violations'], inplace = True)

#Transform y labels

y = y.apply(lambda x: '1' if x in ['pass'] else '0' ) 

