#!/usr/bin/env python
"""
Script to query the most recent 200 records from the flight_data table.
"""
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
import argparse

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Query recent flight data')
    parser.add_argument('--limit', type=int, default=200, help='Number of records to retrieve')
    parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    parser.add_argument('--sqlite-path', default='flight_data.db', help='Path to SQLite database')
    parser.add_argument('--output', default='recent_flights.csv', help='Output CSV file name')
    return parser.parse_args()

def main():
    """Main function to query and display recent flight data."""
    args = parse_args()
    
    # Load environment variables
    load_dotenv()
    
    if args.sqlite:
        # Connect to SQLite database
        print(f"Connecting to SQLite database at {args.sqlite_path}")
        engine = create_engine(f"sqlite:///{args.sqlite_path}")
        # Regular column names for SQLite
        query = f"""
        SELECT 
            id,
            icao24, 
            callsign,
            estDepartureAirport, 
            estArrivalAirport, 
            firstSeen, 
            lastSeen,
            flight_duration_minutes,
            total_distance_km,
            airport_pair
        FROM 
            flight_data
        ORDER BY 
            lastSeen DESC
        LIMIT {args.limit}
        """
    else:
        # Get PostgreSQL connection details from environment variables
        DB_USER = os.getenv("DB_USER", "postgres")
        DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
        DB_HOST = os.getenv("DB_HOST", "db-raw-opensky.cjeesaow22lr.eu-central-1.rds.amazonaws.com")
        DB_PORT = os.getenv("DB_PORT", "5432")
        DB_NAME = os.getenv("DB_NAME", "opensky")
        
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL database at {DB_HOST}")
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        
        # PostgreSQL query with quoted column names for case sensitivity
        query = f"""
        SELECT 
            id,
            "icao24", 
            "callsign",
            "estDepartureAirport", 
            "estArrivalAirport", 
            "firstSeen", 
            "lastSeen",
            "flight_duration_minutes",
            "total_distance_km",
            "airport_pair"
        FROM 
            flight_data
        ORDER BY 
            "lastSeen" DESC
        LIMIT {args.limit}
        """
    
    try:
        # Execute query and load into DataFrame
        df = pd.read_sql(query, engine)
        
        # Convert Unix timestamps to readable datetime
        if 'firstSeen' in df.columns:
            df['firstSeen_time'] = pd.to_datetime(df['firstSeen'], unit='s')
        if 'lastSeen' in df.columns:
            df['lastSeen_time'] = pd.to_datetime(df['lastSeen'], unit='s')
        
        # Display summary information
        print(f"\nRetrieved {len(df)} records from flight_data table")
        print(f"\nColumns in the result: {', '.join(df.columns)}")
        
        if not df.empty:
            print("\nRecord count by departure airport:")
            departure_column = 'estDepartureAirport'
            if departure_column in df.columns:
                departure_counts = df[departure_column].value_counts().head(10)
                for airport, count in departure_counts.items():
                    print(f"  {airport}: {count} flights")
            else:
                print("  No departure airport column found")
            
            print("\nMost recent flights:")
            recent_flights = df.head(5)
            for _, flight in recent_flights.iterrows():
                print(f"  {flight.get('callsign', 'N/A')} from {flight.get('estDepartureAirport', 'N/A')} to {flight.get('estArrivalAirport', 'N/A')} ({flight.get('lastSeen_time', 'N/A')})")
            
            # Save to CSV
            df.to_csv(args.output, index=False)
            print(f"\nFull results saved to {args.output}")
        else:
            print("\nNo records found")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()