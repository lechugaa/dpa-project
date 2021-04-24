#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 17 12:17:04 2021

@author: mario
"""
import pickle
import pandas as pd
from src.pipeline.limpieza_feature_eng import DataCleaner

## Definir Funciones Step Other

def step_other_train(df, cols, dataset = False):
    
    others_dict = {}
    df_copy = df.copy()
    
    for col in cols:
        lvls = df[col].value_counts(normalize=True)[df[col].value_counts(normalize=True) > 0.002].index.array

        if dataset:
            df_copy[col] = df[col].mask(~df[col].isin(lvls), 'other')
            out = df_copy
        
        else:
            others_dict[col] = lvls
            out = others_dict
            
    return out
 
def step_other_test(df, d_train, cols):
    
    """
    Parameters
    ----------
    df : DataFrame
        Testing DF to apply step_other to.
    d_train : either dict or DF
        Provide a dictionary or a training DF to obtain the dictionary from.
    cols : array, str
        Columns to perform step_other to.
        
    Returns
    -------
    df_copy : DF
        df with columns changed with step_other

    """
    
    #Validar si se esta proveyendo un data frame o un dictionary
    
    if isinstance(d_train, pd.core.frame.DataFrame):
        tmp_dict = step_other_train(d_train, cols, False)
    
    elif isinstance(d_train, dict):
        tmp_dict = d_train
        
    else:
        raise Exception("Either dictionary or training DF must be provided")
        
        
    df_copy = df.copy()
    
    for col in cols:
    
        df_copy[col] = df[col].mask(~df[col].isin(tmp_dict[col]), 'other')
    
    return df_copy

#Importar data
d = DataCleaner()
clean_dset = d.get_clean_df()

clean_dset.shape

#pickle.dump(clean_dset, open('temp/historic-clean-2021-02-22.pkl', 'wb'))
#clean_dset = pd.DataFrame(pickle.load(open('temp/historic-clean-2021-02-22.pkl', 'rb')))

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


other_dict = step_other_train(X_train, ['inspection_type', 'facility_type','zip'], False) #Gets dictionary

other_dict

X_train = step_other_train(X_train, ['inspection_type', 'facility_type','zip'], True) #Gets dataset
X_test = step_other_test(X_test, other_dict, ['inspection_type', 'facility_type','zip'])

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
