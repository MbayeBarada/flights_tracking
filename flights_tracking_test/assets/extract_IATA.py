import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

def create_IATA_col(airports_path: str):
    """
    Retrieve airports csv data and extract iata_code column

    Args:
        path(str): Path to csv file location.
    Returns:
        python list: ISO IATA codes of airports.
    """
    df_airports = pd.read_csv(airports_path)
    IATA_list = df_airports['iata_code'].tolist()

    return IATA_list

#iata_list = create_IATA_col('C:/Users/bigge/DEC/flights_tracking/data/airports.csv')

#print(iata_list)