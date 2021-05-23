#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 22 11:09:27 2021

@author: mario
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px 
import pickle as pkl

import pandas as pd

with  open('../temp/predictions-consecutive-inspections-2021-05-12.pkl', 'rb') as f: predictions_consec = pkl.load(f)

with  open('../temp/model-selection-predicted-scores.pkl', 'rb') as f: predictions_model = pkl.load(f)

predictions_model = pd.DataFrame(predictions_model)
predictions_model.columns = ['comp_score', 'score']

fig2 = px.histogram(predictions_model, x = 'score', nbins = 20, histnorm= 'probability',
                   marginal = 'rug', 
                   title = "Distribución scores: Model")

app = dash.Dash()

app.layout = html.Div([
    html.H2("Comparación de distribuciones de Scores"),
    dcc.Dropdown(
        id = "dropdown",
        options = [{'label': 'count', 'value': ''},
                   {'label': 'probability', 'value': 'probability'}],
        value = 'probability',
        multi = False
        
    ),
    dcc.Graph(id = "preds_plot"),
    dcc.Graph(id = "model_plot")
])


@app.callback(
    Output("preds_plot", "figure"),
    [Input("dropdown", "value")]
    )

def update_hist(hist_type):
    fig = px.histogram(predictions_consec, x = 'score', nbins = 20, histnorm= hist_type,
                   marginal = 'rug', 
                   title = "Predicciones consecutivas")
    
    return fig

@app.callback(
    Output("model_plot", "figure"),
    [Input("dropdown", "value")]
    )

def update_hist2(hist_type):
    fig = px.histogram(predictions_model, x = 'score', nbins = 20, histnorm= hist_type,
                   marginal = 'rug', 
                   title = "Modelo entrenado")
    
    return fig



app.run_server(debug=True)