#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 25 21:29:43 2021

@author: mario
"""
#import pandas as pd
from datetime import datetime
from src.pipeline.limpieza_feature_eng import DataEngineer, DataCleaner


c = DataCleaner(True, datetime.strptime('2021-02-22', "%Y-%m-%d"))
c.clean_data(save = True)
c.get_clean_df()    


d = DataEngineer(True, datetime.strptime('2021-02-22', "%Y-%m-%d" ), True)
d.generate_features(save_df=True)

X_tr, X_tt, y_tr, y_tt = d.get_featured_df()

y_tr.value_counts()
y_tt.value_counts()

X_tr.columns

#d.get_trained_transformers()

e = DataEngineer(False, datetime.strptime('2021-03-08', "%Y-%m-%d" ), False)

e.generate_features(save_df=True, save_transformers=True)
e.get_featured_df()
e.get_trained_transformers()


