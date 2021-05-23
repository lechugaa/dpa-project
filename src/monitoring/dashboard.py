import dash
import os
import re
import datetime
import plotly.express as px
import pickle as pkl
import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output


paths = [f for f in os.listdir('temp') if re.match(r'predictions-consecutive-inspections-', f)]
date_fmt = "%Y-%m-%d"
dates = [datetime.datetime.strptime(path.split('.')[0][-10:], date_fmt) for path in paths]
predictions_path = f"temp/predictions-consecutive-inspections-{max(dates).strftime(date_fmt)}.pkl"

with open(predictions_path, 'rb') as f:
    predictions_consec = pkl.load(f)

with open('temp/model-selection-predicted-scores.pkl', 'rb') as f:
    predictions_model = pkl.load(f)

predictions_model = pd.DataFrame(predictions_model)
predictions_model.columns = ['comp_score', 'score']

fig2 = px.histogram(predictions_model, x='score', nbins=20, histnorm='probability',
                    marginal='rug',
                    title="Distribución scores: Model")

app = dash.Dash()

app.layout = html.Div([
    html.H2("Comparación de distribuciones de Scores"),
    dcc.Dropdown(
        id="dropdown",
        options=[{'label': 'count', 'value': ''},
                 {'label': 'probability', 'value': 'probability'}],
        value='probability',
        multi=False

    ),
    dcc.Graph(id="preds_plot"),
    dcc.Graph(id="model_plot")
])


@app.callback(Output("preds_plot", "figure"), [Input("dropdown", "value")])
def update_hist(hist_type):
    fig = px.histogram(predictions_consec, x='score', nbins=20, histnorm=hist_type, marginal='rug',
                       title="Predicciones consecutivas")
    return fig


@app.callback(Output("model_plot", "figure"), [Input("dropdown", "value")])
def update_hist2(hist_type):
    fig = px.histogram(predictions_model, x='score', nbins=20, histnorm=hist_type, marginal='rug',
                       title="Modelo entrenado")
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)