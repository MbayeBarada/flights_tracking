# flights_tracking

## OpenSky ETL Pipeline

A modular ETL (Extract, Transform, Load) pipeline for processing flight data from the OpenSky Network API. This pipeline extracts flight information, applies transformations to derive additional metrics, and loads the processed data into a PostgreSQL database.
Overview

The OpenSky ETL Pipeline extracts flight data from the OpenSky Network API, which provides real-time and historical data about aircraft around the world. The pipeline processes this data to derive insights such as flight durations, distances traveled, and airport connection patterns. It supports both full and incremental loading, allowing for efficient processing of only new data since the last run.

## Features

1. Modular Architecture: Clean separation of extract, transform, and load processes

2. Incremental Loading: Efficiently processes only new data since the last run

3. Configurable: Environment variables for easy configuration

4. SQL Transformations: Uses SQL files with Jinja2-style templating

5. Containerized: Docker support for consistent deployment

6. AWS Ready: Designed for deployment on Amazon ECS with scheduled execution

7. Comprehensive Logging: Detailed logging throughout the ETL process

8. Error Handling: Robust error handling with fallback mechanisms

### Additional files:

- ```Dockerfile```: Container definition for deployment
- ```main.py```: Entry point for running the ETL pipeline
```requirements.txt```: Python dependencies
- ```.env```.template: Template for environment variables
- ```read.py```: Utility script to query recent flights

## Installation

- Prerequisites
    - Python 3.9+
    - PostgreSQL database
    - OpenSky Network API credentials
    
## Local Setup

1. Clone the repository:
```git clone https://github.com/yourusername/opensky-etl.git```
```cd opensky-etl ```
2. Create a virtual environment and install dependencies:
```python -m venv venv```
```source venv/bin/activate```  
#### On Windows: ```venv\Scripts\activate```

```pip install -r requirements.txt```

3. Create a .env file from the template:

```cp .env.template .env```

Edit the .env file with your OpenSky API credentials and database connection details.

## Configuration

The following environment variables can be configured in your .env file:
### OpenSky API credentials
```OPENSKY_USERNAME=your_opensky_username```

```OPENSKY_PASSWORD=your_opensky_password```

## Database configuration
```DB_USER=postgres```

```DB_PASSWORD=your_db_password```

```DB_HOST=your-db-host.amazonaws.com```

```DB_PORT=5432```

```DB_NAME=opensky```

## Usage

### Running the ETL Pipeline

- To run the ETL pipeline with incremental loading:
```python main.py```

- To force a full load of all data:
```python main.py --full```

- To adjust the logging level:
```python main.py --log-level DEBUG```

### Querying Recent Flights
To query and view the most recent flights in the database:
```python read.py```

## Additional options:
### Get the last 500 records
```python read.py --limit 500```

### Use SQLite instead of PostgreSQL
```python read.py --sqlite```

### Save to a specific output file
```python read.py --output latest_flights.csv```

## Data Transformations
The pipeline includes three key transformations:
1. **Flight Duration Calculation** 
Calculates the duration of each flight in minutes:
```
CASE 
    WHEN firstSeen IS NOT NULL AND lastSeen IS NOT NULL
    THEN (lastSeen - firstSeen) / 60.0  -- Convert seconds to minutes
    ELSE NULL
END AS flight_duration_minutes
```
2. **Flight Distance Calculation**
Estimates the total distance traveled in kilometers:
```
CASE 
    WHEN estDepartureAirportHorizDistance IS NOT NULL 
        AND estArrivalAirportHorizDistance IS NOT NULL
    THEN (estDepartureAirportHorizDistance + estArrivalAirportHorizDistance) / 1000.0
    ELSE NULL
END AS total_distance_km
```
3. **Airport Pairs**
Creates an identifier for each origin-destination pair:
```
CASE 
    WHEN estDepartureAirport IS NOT NULL AND estArrivalAirport IS NOT NULL
    THEN CONCAT(estDepartureAirport, '-', estArrivalAirport)
    ELSE NULL
END AS airport_pair
```

## Deployment

### Docker Deployment
**Build the Docker image:**

```docker build -t opensky-etl .```

**Run the container**:

```docker run --env-file .env opensky-etl```

### AWS Deployment
#### Setting up ECR Repository

**Create a new repository in Amazon ECR:**

```aws ecr create-repository --repository-name opensky-etl```

**Authenticate Docker to your ECR registry:**

```aws ecr get-login-password --region your-region | docker login --username AWS --password-stdin your-account-id.dkr.ecr.your-region.amazonaws.com```

**Tag and push your image:**

```docker tag opensky-etl:latest your-account-id.dkr.ecr.your-region.amazonaws.com/opensky-etl:latest```

```docker push your-account-id.dkr.ecr.your-region.amazonaws.com/opensky-etl:latest```

### Setting up ECS Task Definition
> Create a task definition for your container

> Configure environment variables for OpenSky API and database credentials

> Set appropriate CPU and memory limits

### Setting up Scheduled Task

> Create an EventBridge rule to run your ETL pipeline on a schedule

> Set the target to your ECS task definition

>Configure a schedule expression (e.g., cron(0 0 * * ? *) for daily at midnight)

### Monitoring
#### Logs
The ETL pipeline logs detailed information about each stage of the process.
When deployed on AWS, logs are available in CloudWatch Logs.

Local runs create logs in the logs/ directory
Database Views. 

The pipeline creates two views for monitoring and analysis

- airport_departures: Summary of flights by departure airport

- flight_durations: Detailed analysis of flight durations and distances

## Troubleshooting

- Common Issues
    - PostgreSQL Connection Errors: Check your database credentials and security group settings
    - OpenSky API Authentication Errors: Verify your OpenSky username and password
    - ```NumericValueOutOfRange``` Errors: Ensure database column types are appropriate for the data
    - **PostgreSQL Case Sensitivity**
PostgreSQL converts column names to lowercase unless they're quoted. When working with PostgreSQL directly, use double quotes around column names:

    ```
    SELECT "estDepartureAirport", "flight_duration_minutes"
    FROM flight_data
    ```
## License
This project is licensed under the MIT License - see the LICENSE file for details.