""" @bruin

name: materialize.country

materialization:
    type: table


connection: duckdb-default
@bruin """

import pandas as pd
import numpy as np

def materialize():
    # Generate random country data
    n_rows = 50  # Number of sample countries
    
    # Create sample data
    data = {
        'country_name': f'Country_{np.random.randint(1000000, 500000000, n_rows)}',
        'population': np.random.randint(1000000, 500000000, n_rows),
        'gdp': np.random.uniform(1000, 50000, n_rows),
        'country': "India",
        'area': np.random.randint(10000, 1000000, n_rows)
    }    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Return the dataset
    return df


