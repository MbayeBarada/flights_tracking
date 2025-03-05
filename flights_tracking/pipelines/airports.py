from dotenv import load_dotenv
import os
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from flights_tracking.connectors.postgresql import PostgreSqlClient
from flights_tracking.assets.airports import extract_airports_data
from flights_tracking.assets.airports import load


if __name__ == "__main__":
    load_dotenv()
    
    API_KEY = os.environ.get("API_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")
    

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    df = extract_airports_data("flights_tracking/data/airports.csv")
    
    
    metadata = MetaData()
    table = Table(
        "airports",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("ident", String, primary_key=True),
        Column("type", String),
        Column("name", String),
        Column("latitude_deg", Float),
        Column("longitude_deg", Float),
        Column("elevation_ft", Float),
        Column("continent", String),
        Column("iso_country", String),
        Column("iso_region", String),
        Column("municipality", String),
        Column("scheduled_service", String),
        Column("icao_code", String),
        Column("iata_code", String),
        Column("gps_code", String),
        Column("local_code", String)    
    )
    
    load(
        df=df,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )