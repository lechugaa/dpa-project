#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 17 12:17:04 2021

@author: mario
"""
import pickle
import pandas as pd
from src.pipeline.limpieza_feature_eng import DataCleaner

## Definir Funcion Step Other

def step_other(df_train, df_test, cols, thresh = 0.002):
    
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

#Importar data
d = DataCleaner()
clean_dset = d.get_clean_df()

clean_dset.shape

#pickle.dump(clean_dset, open('temp/historic-clean-2021-02-22.pkl', 'wb'))
clean_dset = pd.DataFrame(pickle.load(open('temp/historic-clean-2021-02-22.pkl', 'rb')))

from sklearn.model_selection import train_test_split
#split dataset

y = clean_dset['results'] 
X = clean_dset.drop(columns = 'results')

X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle = False)


# Extraer atributos de fechas

X_train['dow'] = X_train['inspection_date'].dt.day_name().str.lower()
X_train['month'] = X_train['inspection_date'].dt.month
X_train = X_train.astype({"month": 'str'})

X_test['dow'] = X_test['inspection_date'].dt.day_name().str.lower()
X_test['month'] = X_test['inspection_date'].dt.month
X_test = X_test.astype({"month": 'str'})


#Step other
X_train, X_test, other_dict = step_other(X_train, X_test, ['inspection_type', 'facility_type','zip'])

#Escalar lat y long
from sklearn.preprocessing import MinMaxScaler 

scaler = MinMaxScaler()
X_train[['longitude','latitude']] = scaler.fit_transform(X_train[['longitude','latitude']])
X_test[['longitude','latitude']] = scaler.transform(X_test[['longitude','latitude']])

#One hot encoding

from sklearn.preprocessing import OneHotEncoder

encoder = OneHotEncoder(sparse = False)


onehot_train = pd.DataFrame(encoder.fit_transform(X_train[['inspection_type', 'facility_type', 'zip', 'risk','dow','month']]))
onehot_train.columns = encoder.get_feature_names(['inspection_type', 'facility_type', 'zip', 'risk','dow','month'])
onehot_train

X_train.drop(columns = ['inspection_type', 'facility_type', 'zip', 'risk','dow','month'], inplace= True)
X_train = pd.concat([X_train, onehot_train], axis = 1)
X_train

onehot_test = pd.DataFrame(encoder.transform(X_test[['inspection_type', 'facility_type', 'zip', 'risk','dow','month']]))
onehot_test.columns = encoder.get_feature_names(['inspection_type', 'facility_type', 'zip', 'risk','dow','month'])
onehot_test

X_test.drop(columns = ['inspection_type', 'facility_type', 'zip', 'risk','dow','month'], inplace= True)

X_test = pd.concat([X_test.reset_index(drop = True), onehot_test], axis = 1)

X_test

#Drop useless columns

X_train.drop(columns = ['inspection_id', 'inspection_date', 'violations'], inplace = True)
X_test.drop(columns = ['inspection_id', 'inspection_date', 'violations'], inplace = True)


X_train
X_test


#Transform y labels

y_train = y_train.apply(lambda x: '1' if x in ['pass'] else '0' ) 

y_train = y_test.reset_index(drop = True).apply(lambda x: '1' if x in ['pass'] else '0' )
