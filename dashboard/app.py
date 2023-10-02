import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
import numpy as np

# Generate random sample data
np.random.seed(0)
df = pd.DataFrame({'x': np.random.rand(50), 'y': np.random.rand(50)})

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    dcc.Graph(id='scatter-plot'),
])

# Define callback to update the scatter plot
@app.callback(
    dash.dependencies.Output('scatter-plot', 'figure'),
    []
)
def update_scatter_plot():
    fig = px.scatter(df, x='x', y='y', title='Sample Scatter Plot')
    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
