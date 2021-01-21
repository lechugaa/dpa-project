#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 12:26:33 2021

@author: mario
"""

import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

dta = pd.read_csv('data/Food_Inspections.csv' )

dta.columns = dta.columns.str.lower().str.replace(' #','').str.replace(' ','_')

dta.columns

#Procesar fechas

pd.to_datetime(dta['inspection_date']).isna().value_counts()

dta['inspection_date'] = pd.to_datetime(dta['inspection_date'])

dta['inspection_month'] = dta['inspection_date'].dt.month
dta['inspection_year'] = dta['inspection_date'].dt.year


#procesar variables categóricas

dta['facility_type'].unique().size
dta['facility_type'].value_counts().head(20)

top_facility_types = dta['facility_type'].value_counts().head(16).index.array


dta['inspection_type'].unique().size
dta['inspection_type'].value_counts().head(20)
top_inspection_types = dta['inspection_type'].value_counts().head(16).index.array


dta['violations'].unique().size
dta['violations'].value_counts().head(16)
#Nada bueno sale de aquí 

dta.results.unique()

dta['results'] = np.where(dta['results'].isin(['Pass','Pass w/ Conditions']) , 'Pass', 'Not pass' ) 

# -Serie de tiempo de inspecciones

inspecciones_diarias = dta.groupby(['inspection_date']).size().reset_index(name = "n")

f = plt.subplots(figsize = (15,5))
ax = sns.lineplot(data = inspecciones_diarias, x = 'inspection_date', y = 'n')

inspecciones_diarias_results = dta.groupby(['inspection_date', 'results']).size().reset_index(name = "n")

f = plt.subplots(figsize = (15,5))
ax = sns.relplot(data = inspecciones_diarias_results, x = 'inspection_date',
                 y = 'n', col = 'results', col_wrap=3, kind = 'line')

#Verificar que todos los puntos se encuentran dentro de Chicago
sns.relplot(data= dta, y = "latitude", x = "longitude", hue = 'results', kind= 'scatter', col = 'results', col_wrap = 3)


#Ver proporciones de 
# -Risk
risk_order = dta.risk.value_counts().index
sns.countplot(data = dta, y = 'risk', order = risk_order)
    
# -Results

results_order = dta.results.value_counts().index
sns.countplot(data = dta, y = 'results', order = results_order)


#Cruces de variables:
# -Facility Type vs. Results
dta_filt_facility_type = dta[dta['facility_type'].isin(top_facility_types)]

sns.catplot(data = dta_filt_facility_type, col = 'facility_type', y = 'results', kind = 'count', col_wrap=4,
                col_order = top_facility_types, order = results_order, sharex = False )  

# -Inspection Type vs Results

dta_filt_inspection_type = dta[dta['inspection_type'].isin(top_inspection_types)] 

sns.catplot(data = dta_filt_inspection_type, col = 'inspection_type', y = 'results', kind = 'count', col_wrap=4,
                col_order = top_inspection_types, order = results_order, sharex = False )  

# -Risk vs. Results

sns.catplot(data = dta, col = 'risk', y = 'results', kind = 'count', col_wrap=3,
            order = results_order, col_order = risk_order,sharex = False)  

