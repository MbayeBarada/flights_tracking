from dotenv import load_dotenv
import os
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from flights_tracking.connectors.postgresql import PostgreSqlClient
from flights_tracking.assets.flight_incidents import extract_incidents_data
from flights_tracking.assets.flight_incidents import load


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

    df = extract_incidents_data("flights_tracking/data/AIDS_REPORTS.csv")
    
    metadata = MetaData()
    table = Table(
        "incidents_report",
        metadata,
        Column("AIDS Report Number", String, primary_key=True),
        Column("Local Event Date", String),
        Column("Event City", String),
        Column("Event State", String),
        Column("Event Airport", String),
        Column("Operator", String),
        Column("Flight Conduct Code", String),
        Column("Total Fatalities", Integer),
        Column("Total Injuries", Integer),
        Column("PIC Flight Time Total Hrs", Float),
        Column("PIC Flight Time Total Make-Model", Float)
    )

    load(
        df=df,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )