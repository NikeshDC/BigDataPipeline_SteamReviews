import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from kafka import KafkaConsumer
import json
import threading
from dateutil import parser as date_parser

# Kafka Configuration
KAFKA_SERVERS = 'localhost:9092'
SOURCE_TOPIC = 'summary-results'


# Initialize global dataframes to store the incoming data
dummy_data = {
    "app_id" : [292030],
    "app_name" : ['The Witcher 3: Wild Hunt'],
    "time_year": [2021],
    "time_month": [12],
    "time_day": [29],
    "A_playtime": [7.9],
    "A_sentiment": [0.34],
    "T_reviews": [2],
    "T_recommendations": [2],
    "T_pos_reviews": [2],
    "T_neg_reviews": [0]
}
data_schema = {
    "app_id" : "int32",
    "app_name" : "object",
    "time_year": "int16",
    "time_month": "int8",
    "time_day": "int8",
    "A_playtime": "float32",
    "A_sentiment": "float32",
    "T_reviews": "int32",
    "T_recommendations": "int32",
    "T_pos_reviews": "int32",
    "T_neg_reviews": "int32"
}
summary_data = pd.DataFrame(data=dummy_data)
summary_data = summary_data.astype(data_schema)

options_games = []

# Consume data from Kafka
def consume_kafka_data():
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for message in consumer:
        update_database(message.value)

##----------------------------------------------------------------NEW_DATA_UPDATE--------------------------------------------------------------------------------------

# Functions to update the dataframes
def update_database(new_data):
    global summary_data
    app_id = new_data['app_id']
    time = get_year_month_day(new_data['time'])

    # Extract necessary columns
    condition = ((summary_data['app_id'] == app_id) & 
        (summary_data['time_year'] == time[0]) & (summary_data['time_month'] == time[1]) 
        & (summary_data['time_day'] == time[2]))

    # Check if data for app at that time already exists
    if condition.any():
        # Directly update the existing row
        idx = summary_data[condition].index[0]
        summary_data.loc[idx, :] = extract_to_required_format(new_data, app_id, time).iloc[0]
    else:
        # Append the new data
        new_row = extract_to_required_format(new_data, app_id, time)
        summary_data = pd.concat([summary_data, new_row], ignore_index=True)

    if new_data['app_name'] not in options_games:
        options_games.append(new_data['app_name'])


def get_year_month_day(time):
    date_obj = date_parser.parse(time)
    # Extract year, month, and day
    return date_obj.year, date_obj.month, date_obj.day

def extract_to_required_format(data, app_id, time):
    dummy_data = {
        "app_id" : app_id,
        "app_name" : data['app_name'],
        "time_year": time[0],
        "time_month": time[1],
        "time_day": time[2],
        "A_playtime": data.get('A_playtime', 0.0),
        "A_sentiment": data.get("A_sentiment", 0.0),
        "T_reviews": data.get("T_reviews", 0),
        "T_recommendations": data.get("T_recommendations", 0),
        "T_pos_reviews": data.get("T_pos_reviews", 0),
        "T_neg_reviews": data.get("T_neg_reviews", 0)
    }
    return pd.DataFrame([dummy_data])

##--------------------------------------------------------------------------------------------------------------------------------------------------------------


##----------------------------------------------------------------DASH_APPLICATION--------------------------------------------------------------------------------------
# Initialize Dash App
app = dash.Dash(__name__)

# Create dropdown options based on unique games
def generate_game_options():
    if options_games.count == 0:
        return []
    return [{'label': game, 'value': game} for game in options_games]

##--------------------------------------------------------------DASH_APPLICATION_LAYOUT--------------------------------------------------------------------------------------
# Layout for the dashboard
app.layout = html.Div([
    html.H1("Steam Game Reviews - Popularity & Sentiment Analysis"),

    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # 5 seconds
        n_intervals=0
    ),

    # Dropdown to select a game for sentiment analysis
    html.Div([
        html.Label("Select Game for Sentiment Analysis"),
        dcc.Dropdown(
            id='game-dropdown',
            options=[],  # Initial empty options
            placeholder="Select a game"
        )
    ]),

    # Dropdown to select a popularity metric for ranking
    html.Div([
        html.Label("Select Metric"),
        dcc.Dropdown(
            id='metric-dropdown',
            options=[
                {'label': 'Average Playtime', 'value': "A_playtime"},
                {'label': 'Total Reviews', 'value': "T_reviews"},
                {'label': 'Total Positive Reviews', 'value': "T_pos_reviews"},
                {'label': 'Total Negative Reviews', 'value': "T_neg_reviews"},
                {'label': 'Total Recommendations', 'value': "T_recommendations"},
                {'label': 'Average Sentiment', 'value': "A_sentiment"}
            ],
            value='A_playtime',  # Default value
            placeholder="Select a metric to rank games"
        )
    ]),

    # Graph for time-based analysis for selected game from above dropdown
    html.Div([
        dcc.Graph(id='time-graph')
    ]),

    # Dropdown to select a popularity metric for ranking
    html.Div([
        html.Label("Select Time"),
        html.Div([
            html.Label("Year: "),
            dcc.Input(id="input-time-year", value=2020, type="number", min=2000, max=2025, debounce = True)
        ]),
        html.Div([
            html.Label("Month: "),
            dcc.Input(id="input-time-month", value=1, type="number", min= 0, max = 12, debounce = True)
        ]),
        html.Div([
            html.Label("Day: "),
            dcc.Input(id="input-time-day", value=1, type="number", min=0, max=32, debounce = True)
        ])
    ]),

    # Graph for ranked popularity based on metric selected
    html.Div([
        dcc.Graph(id='popularity-graph')
    ])
])

##--------------------------------------------------------------DASH_APPLICATION_CALLBACKS--------------------------------------------------------------------------------------
#update time graph based on selected game or metric
@app.callback(
    Output('time-graph', 'figure'),
    Input('game-dropdown', 'value'),
    Input('metric-dropdown', 'value')
)
def update_time_graph(selected_game, selected_metric):
    if selected_game is None:
        return {}
    
    if 'A_' in selected_metric:
        agg_func = 'mean'  ##aggregate average
    else:
        agg_func = 'sum'  ##aggregate total
                                
    # Filter the data for the selected game
    selected_data = summary_data.loc[summary_data['app_name'] == selected_game, ['app_name', 'time_year', 'time_month',selected_metric]] 
    selected_data = selected_data.groupby(by=['app_name', 'time_year', 'time_month']) \
                                .aggregate(agg_func) \
                                .reset_index() \
                                .sort_values(by = ['time_year', 'time_month'])
    selected_data['time'] = selected_data['time_year'].astype(str) + '/' + selected_data['time_month'].astype(str)

    metric_name = selected_metric.replace('A_', "Average").replace('T_', 'Total ')

    # Sentiment over time plot
    time_fig = px.line(
        selected_data,
        x='time',
        y=selected_metric,
        title=f"{metric_name} for {selected_game} Over Time",
        labels={'time': 'Time',
                selected_metric: metric_name}
    )
    return time_fig


#update popularity graph based on selected metric
@app.callback(
    Output('popularity-graph', 'figure'),
    Input('metric-dropdown', 'value'),
    Input('input-time-year', 'value'),
    Input('input-time-month', 'value'),
    Input('input-time-day', 'value')
)
def update_popularity_graph(selected_metric, selected_year, selected_month, selected_day):
    if selected_metric is None:
        return {}

    if 'A_' in selected_metric:
        agg_func = 'mean'  ##aggregate average
    else:
        agg_func = 'sum'  ##aggregate total

    if not selected_year:
        #aggregate all values
        selected_data = summary_data
    if not selected_month:
        #aggregate based on selected year
        selected_data = summary_data.loc[summary_data['time_year'] ==  selected_year, :] 
    elif not selected_day:
        #aggregate based on selected year, month
        selected_data = summary_data.loc[(summary_data['time_year'] == selected_year) & (summary_data['time_month'] == selected_month), :] 
    else:
        selected_data = summary_data.loc[(summary_data['time_year'] == selected_year) & 
                                         (summary_data['time_month'] == selected_month) & (summary_data['time_day'] == selected_day), :] 

    # Sort the data by the selected metric
    if sorted_popularity_data.size == 0:
        return {}
    
    sorted_popularity_data = selected_data[['app_name', selected_metric]] \
                                .groupby(by=['app_name']) \
                                .aggregate(agg_func) \
                                .sort_values(by=selected_metric, ascending=False) \
                                .reset_index()

    metric_name = selected_metric.replace('A_', "Average ").replace('T_', 'Total ')
    # Popularity ranking plot
    popularity_fig = px.bar(
        sorted_popularity_data,
        x='app_name',
        y=selected_metric,
        title=f"Ranking Games by {metric_name}",
        labels={'app_name': 'Game Name',
                selected_metric: metric_name}
    )
    return popularity_fig


# Callback to update the game dropdown when new sentiment data is added
@app.callback(
    Output('game-dropdown', 'options'),
    [Input('interval-component', 'n_intervals')]
)
def update_game_dropdown(_):
    return generate_game_options()

##---------------------------------------------------------------------------------------------------------------------------------------------------------------

# Run the Dash app
if __name__ == '__main__':
    # Start Kafka consumer in a separate thread
    kafka_thread = threading.Thread(target=consume_kafka_data)
    kafka_thread.start()

    app.run_server(host='0.0.0.0', port='8085', debug=True)