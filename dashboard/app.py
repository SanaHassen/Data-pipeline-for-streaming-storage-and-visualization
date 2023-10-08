from dash import Dash, html, dash_table, dcc
from dash.dependencies import Input, Output, State
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, external_stylesheets=external_stylesheets)

# Variable Initialization
df_bikes = None
df_shops = None
df_orders = None
product_join = None
grouped_quantity = None
grouped_products_price = None

#Creating a Spark Session
spark = SparkSession.builder.appName("HDFSReadMultiplePaths").getOrCreate()

# Define a function to read data from HDFS 
def read_data_from_hdfs():
    global df_bikes
    global df_shops
    global df_orders
    global product_join
    global grouped_quantity
    global grouped_products_price

    # Data Dictionary Initialization
    data_bikes = {'bike.id': [], 'model': [], 'category1': [] , 'category2': [], 'frame': [], 'price': []}
    data_shops =  {'bikeshop.id': [], 'bikeshop.name': [], 'bikeshop.city': [] , 'bikeshop.state': [], 'latitude': [], 'longitude': []}
    data_orders =  {'index': [], 'order.id': [], 'order.line': [], 'order.date': [] , 'customer.id': [], 'product.id': [], 'quantity': []}
    
    # HDFS File Paths
    hdfs_path_bikes = "hdfs://hdfs-namenode:9000/data/bikes_result"
    hdfs_path_bikeshops = "hdfs://hdfs-namenode:9000/data/bikeshops_result"
    hdfs_path_orders = "hdfs://hdfs-namenode:9000/data/orders_result"

    # Reading Data from HDFS using read.csv since data in saved in csv format
    df_bikes_hdfs = spark.read.csv(
        hdfs_path_bikes, header=False, inferSchema=True, sep=',')
    df_shops_hdfs = spark.read.csv(
        hdfs_path_bikeshops, header=False, inferSchema=True, sep=',')
    df_orders_hdfs = spark.read.csv(
        hdfs_path_orders, header=False, inferSchema=True, sep=',')
    
    # Loop Over Rows
    for row_iterator in df_bikes_hdfs.collect():
        data = row_iterator['_c1'].split(';') # in hdfs, we have 3 columns key, value, topic : we choose the value coluln wehere we have the data stored in string comma seperated format
        data_bikes['bike.id'].append(data[0])
        data_bikes['model'].append(data[1])
        data_bikes['category1'].append(data[2])
        data_bikes['category2'].append(data[3])
        data_bikes['frame'].append(data[4])
        data_bikes['price'].append(float(data[5]))
  

    for row_iterator in df_shops_hdfs.collect():
        data = row_iterator['_c1'].split(';')
        data_shops['bikeshop.id'].append(data[0])
        data_shops['bikeshop.name'].append(data[1])
        data_shops['bikeshop.city'].append(data[2])
        data_shops['bikeshop.state'].append(data[3])
        data_shops['latitude'].append(data[4])
        data_shops['longitude'].append(data[5])
  

    for row_iterator in df_orders_hdfs.collect():
        data = row_iterator['_c1'].split(';')
        data_orders['index'].append(data[0])
        data_orders['order.id'].append(int(data[1]))
        data_orders['order.line'].append(data[2])
        data_orders['order.date'].append(data[3])
        data_orders['customer.id'].append(str(data[4]))
        data_orders['product.id'].append(str(data[5]))
        data_orders['quantity'].append(int(data[6]))


    df_bikes = pd.DataFrame.from_dict(data_bikes)
    df_shops = pd.DataFrame.from_dict(data_shops)
    df_orders = pd.DataFrame.from_dict(data_orders)

    df_shops['latitude'].replace(',', '.', inplace=True, regex=True)
    df_shops['longitude'].replace(',', '.', inplace=True, regex=True)
    df_shops['latitude'] = pd.to_numeric(df_shops['latitude'])
    df_shops['longitude'] = pd.to_numeric(df_shops['longitude'])

    df_orders['order.date'] = pd.to_datetime(df_orders['order.date'])
    df_orders['date'] = df_orders['order.date'].dt.date
    df_orders['year'] = df_orders['order.date'].dt.year

    grouped_quantity = df_orders.groupby(['year'])[
        'quantity'].agg('sum').reset_index()

    product_join = df_orders.set_index(
        'product.id').join(df_bikes.set_index('bike.id'), lsuffix='_orders') #'lsuffix='_orders'' suffix is added to columns with the same name in both DataFrames to distinguish them.

    grouped_products_price = product_join.groupby(['year'])[
        'price'].agg('sum').reset_index()

read_data_from_hdfs()

# Create differents figures

# Quantity per day chart
@app.callback(
    Output('scatter-chart-quantity', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_line_chart(start_date, end_date):
    filtered_df = df_orders[(df_orders['order.date'] >= start_date) & (
        df_orders['order.date'] <= end_date)]
    monthly_agg = filtered_df.resample(
        'D', on='order.date').sum().reset_index()

    fig = px.scatter(monthly_agg, x='order.date', y='quantity', labels={
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
    filtered_df = df_orders[(df_orders['order.date'] >= start_date) & (
        df_orders['order.date'] <= end_date)]

    grouped_quantity = filtered_df.groupby(['customer.id'])[
        'quantity'].agg('sum').reset_index()

    fig = px.bar(grouped_quantity.nlargest(10, 'quantity'), x='customer.id', y='quantity', labels={
        'customer.id': 'Customer', 'quantity': 'Quantity'}, title="Top 10 Customers")

    fig.update_xaxes(type='category')
    return fig

# Top products Chart
@app.callback(
    Output('top-products-bar', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_line_chart(start_date, end_date):
    filtered_df = df_orders[(df_orders['order.date'] >= start_date) & (
        df_orders['order.date'] <= end_date)]

    grouped_quantity = filtered_df.groupby(['product.id'])[
        'quantity'].agg('sum').reset_index()
    joined = grouped_quantity.set_index(
        'product.id').join(df_bikes.set_index('bike.id'))

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
    filtered_df = df_orders[(df_orders['order.date'] >= start_date) & (
        df_orders['order.date'] <= end_date)]

    grouped_quantity = filtered_df.groupby(['order.line'])[
        'quantity'].agg('sum').reset_index()
    joined = grouped_quantity.set_index(
        'order.line').join(df_shops.set_index('bikeshop.id'))

    fig = px.bar(joined.nlargest(10, 'quantity'), x='bikeshop.name', y='quantity', labels={
        'bikeshop.name': 'Shop', 'quantity': 'Quantity'}, title="Top 10 Shops")

    fig.update_xaxes(type='category')
    return fig

# Shops map
@app.callback(
    Output('top-shops-heat', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_line_chart(start_date, end_date):

        filtered_df = df_orders[(df_orders['order.date'] >= start_date) & (
            df_orders['order.date'] <= end_date)]

        grouped_quantity = filtered_df.groupby(['order.line'])[
            'quantity'].agg('sum').reset_index()

        joined = grouped_quantity.set_index(
            'order.line').join(df_shops.set_index('bikeshop.id'))

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
                'order.date', 'order.line',], page_size=10, ),
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
