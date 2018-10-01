import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import data_controller

df = pd.read_csv(
    'https://gist.githubusercontent.com/chriddyp/'
    'c78bf172206ce24f77d6363a2d754b59/raw/'
    'c353e8ef842413cae56ae3920b8fd78468aa4cb2/'
    'usa-agricultural-exports-2011.csv')

dc = data_controller.DataController()
dc.exchanges = ['binance']

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

app = dash.Dash()

app.layout = html.Div(children=[
    html.H1(children='Hello Metrics'),

    html.Div(children='''
        Python Metrics Dashboard.
    '''),

    
    dcc.Input(
        id='liquidity_amount',
        placeholder='Enter a amount...',
        type='number',
        value=10000.0,
    ),
    dcc.Dropdown(
        id='market',
        options=[
            {'label': 'BTC/USDT', 'value': 'BTC/USDT'},
            {'label': 'TUSD/USDT', 'value': 'TUSD/USDT'},
            {'label': 'TUSD/BTC', 'value': 'TUSD/BTC'}
        ],
        value='BTC/USDT'
    ),
    dcc.Dropdown(
        id='exchange',
        options=[
            {'label': 'binance', 'value': 'binance'},
            {'label': 'bittrex', 'value': 'bittrex'},
        ],
        value='binance'
    ),
    dcc.Interval(
        id='interval-component',
        interval=1*1000, # in milliseconds
        n_intervals=0
    ),
    html.Div(id='live-update-text'),

    html.H4(children='Stability'),
    # generate_table(df)
])

@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals'),
              Input('market', 'value'),
              Input('liquidity_amount', 'value')])
def update_metrics(n, market, amount):
    # some = dc.big_data['SingleLiquidityProcessor']
    # some[market] = amount
    dc.big_data['SingleLiquidityProcessor'][market] = float(amount)
    print('arstarstarstarstarstarst', dc.big_data['Output'])
    style = {'padding': '5px', 'fontSize': '16px'}
    base, quote = market.split('/')
    return [
        html.Div(),
        html.Span('Buying {} {} costs {} {}'.format(amount, base, dc.big_data['Output']['SingleLiquidityProcessor']['binance'][market]['ask'], quote)),
        html.Div(),
        html.Span('Selling {} {} nets {} {}'.format(amount, base, dc.big_data['Output']['SingleLiquidityProcessor']['binance'][market]['bid'], quote)),
    ]
dc.start()
# print(dc.big_data)
# @app.callback(Output('live-update-text', 'children'),
#               [Input('market', 'value'),
#               Input('liquidity_amount', 'value')])
# def update_params(market, amount):
    
#     style = {'padding': '5px', 'fontSize': '16px'}
#     return [
#         html.Span('Liquidity: {0:.2f}'.format(23), style=style),
        
#     ]
    

if __name__ == '__main__':
    app.run_server(debug=True)