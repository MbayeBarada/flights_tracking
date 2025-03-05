import pandas as pd
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.dialects import postgresql
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from flights_tracking.connectors.postgresql import PostgreSqlClient




def extract_incidents_data(path:str)-> pd.DataFrame:
    """
    Extract incident data from a given file path.

    Args:
        path (str): The file path to the data source.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted incident data.
    """



    faa_data = pd.read_csv(path)
    faa_data = faa_data[
                    ['AIDS Report Number','Local Event Date', 'Event City', 'Event State',
                    'Event Airport', 'Operator', 'Flight Conduct Code', 'Total Fatalities', 'Total Injuries',
                    'PIC Flight Time Total Hrs', 'PIC Flight Time Total Make-Model' ]
]

    return faa_data


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