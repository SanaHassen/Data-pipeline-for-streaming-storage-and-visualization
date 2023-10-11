from dash import Dash, html, dash_table, dcc
from dash.dependencies import Input, Output, State
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import year

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, external_stylesheets=external_stylesheets)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Cassandra Reader") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .getOrCreate()

# Read the Cassandra tables into DataFrames
df_bikes_spark = spark.read.format("org.apache.spark.sql.cassandra").options(table="bikes", keyspace="default").load()
df_bikeshops_spark = spark.read.format("org.apache.spark.sql.cassandra").options(table="bikeshops", keyspace="default").load()
df_orders_spark = spark.read.format("org.apache.spark.sql.cassandra").options(table="orders", keyspace="default").load()

df_orders_spark = df_orders_spark.withColumn('year', year('order.date'))

df_bikes = df_bikes_spark.toPandas()
df_bikeshops = df_bikeshops_spark.toPandas()
df_orders = df_orders_spark.toPandas()


grouped_quantity = df_orders.groupby(['year'])['quantity'].agg('sum').reset_index()

product_join = df_orders.set_index('product_id').join(df_bikes.set_index('id'), lsuffix='_orders') #'lsuffix='_orders'' suffix is added to columns with the same name in both DataFrames to distinguish them.

grouped_products_price = product_join.groupby(['year'])['price'].agg('sum').reset_index()

# Create differents figures

# Quantity per day chart
@app.callback(
    Output('scatter-chart-quantity', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_line_chart(start_date, end_date):
    filtered_df = df_orders[(df_orders['order_date'] >= start_date) & (
        df_orders['order_date'] <= end_date)]
    monthly_agg = filtered_df.resample(
        'D', on='order_date').sum().reset_index()

    fig = px.scatter(monthly_agg, x='order_date', y='quantity', labels={
        'order.date': 'Month', 'quantity': 'Quantity'}, title="Ordered Quantity per Day")
    fig.update_xaxes(type='category')
    return fig

# Top customers Chart
@app.callback(
    Output('top-customers-bar', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_line_chart(start_date, end_date):
    filtered_df = df_orders[(df_orders['order_date'] >= start_date) & (
        df_orders['order_date'] <= end_date)]

    grouped_quantity = filtered_df.groupby(['customer_id'])[
        'quantity'].agg('sum').reset_index()

    fig = px.bar(grouped_quantity.nlargest(10, 'quantity'), x='customer_id', y='quantity', labels={
        'customer_id': 'Customer', 'quantity': 'Quantity'}, title="Top 10 Customers")

    fig.update_xaxes(type='category')
    return fig

# Top products Chart
@app.callback(
    Output('top-products-bar', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_line_chart(start_date, end_date):
    filtered_df = df_orders[(df_orders['order_date'] >= start_date) & (
        df_orders['order_date'] <= end_date)]

    grouped_quantity = filtered_df.groupby(['product_id'])[
        'quantity'].agg('sum').reset_index()
    joined = grouped_quantity.set_index(
        'product_id').join(df_bikes.set_index('bike_id'))

    fig = px.bar(joined.nlargest(10, 'quantity'),  x='model', y='quantity', labels={
        'model': 'Product', 'quantity': 'Quantity'}, title="Top 10 Products")

    fig.update_xaxes(type='category')
    return fig

# Top shops Chart
@app.callback(
    Output('top-shops-bar', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
)
def update_line_chart(start_date, end_date):
    filtered_df = df_orders[(df_orders['order_date'] >= start_date) & (
        df_orders['order_date'] <= end_date)]

    grouped_quantity = filtered_df.groupby(['order_line'])[
        'quantity'].agg('sum').reset_index()
    joined = grouped_quantity.set_index(
        'order_line').join(df_bikeshops.set_index('bikeshop_id'))

    fig = px.bar(joined.nlargest(10, 'quantity'), x='name', y='quantity', labels={
        'name': 'Shop', 'quantity': 'Quantity'}, title="Top 10 Shops")

    fig.update_xaxes(type='category')
    return fig

# Shops map
@app.callback(
    Output('top-shops-heat', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_line_chart(start_date, end_date):

        filtered_df = df_orders[(df_orders['order_date'] >= start_date) & (
            df_orders['order_date'] <= end_date)]

        grouped_quantity = filtered_df.groupby(['order_line'])[
            'quantity'].agg('sum').reset_index()

        joined = grouped_quantity.set_index(
            'order_line').join(df_bikeshops.set_index('bikeshop_id'))

        fig = px.density_mapbox(joined, lat='latitude',
                                radius=20,
                                lon='longitude', title="Sales distribution by City", zoom=2,
                                mapbox_style="stamen-terrain")

        fig.update_xaxes(type='category')
        return fig

# Define the layout of the Dash web application
app.layout = html.Div(style={'margin': '24px'}, children=[

    html.Div(className='row',  children='Dashboard',
             style={'textAlign': 'center', 'color': 'blue', 'fontSize': 30, 'margin': 100}),


    html.Div(className='row', children=[

        html.Div(className='six columns', children=[
            dash_table.DataTable(data=df_orders.to_dict('records'), css=[{"selector": ".show-hide", "rule": "display: none"}], hidden_columns=[
                'order_date', 'order_line',], page_size=10, ),
        ]),
        html.Div(className='six columns', children=[
            dcc.Graph(id='top-shops-heat')
        ])
    ]),

    html.Div(className='row', children=[
        html.Div(className='six columns', children=[
            dcc.Graph(figure=px.line(
                grouped_quantity, x='year', y='quantity', title='Sold quantity by year')),
        ]),
        html.Div(className='six columns', children=[
            dcc.Graph(figure=px.line(
                grouped_products_price, x='year', y='price', title='Total price by year')),
        ])
    ]),
    html.Div(className='row', style={'display': 'flex', 'justifyContent': 'center'}, children=[
        dcc.DatePickerRange(

            id='date-picker-range',
            start_date=df_orders['order.date'].min(),
            end_date=df_orders['order.date'].max(),
            display_format='YYYY-MM-DD',
            style={'display': 'flex', 'position': 'unset'}
        ),
    ]),
    html.Div(className='row', children=[
        dcc.Graph(id='scatter-chart-quantity')
    ]),
    html.Div(className='row', children=[
        html.Div(className='four columns', children=[
            dcc.Graph(id='top-customers-bar')
        ]),
        html.Div(className='four columns', children=[
            dcc.Graph(id='top-products-bar')
        ]),
        html.Div(className='four columns', children=[
            dcc.Graph(id='top-shops-bar')
        ])
    ]),
])

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
