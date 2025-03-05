import pandas as pd
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.dialects import postgresql
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from flights_tracking.connectors.postgresql import PostgreSqlClient




def extract_airports_data(path:str)-> pd.DataFrame:
    """
    Extract airports data from a given file path.

    Args:
        path (str): The file path to the data source.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted incident data.
    """



    airports_data = pd.read_csv(path)
    airports_data = airports_data[
                    ['id', 'ident', 'type', 'name', 'latitude_deg', 'longitude_deg',
                        'elevation_ft', 'continent', 'iso_country', 'iso_region',
                        'municipality', 'scheduled_service', 'icao_code', 'iata_code',
                        'gps_code', 'local_code']
]

    return airports_data


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )