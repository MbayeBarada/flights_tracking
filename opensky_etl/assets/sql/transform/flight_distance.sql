-- Calculate flight distance based on airport distances
-- This transformation calculates a rough estimate of distance traveled

WITH flight_data_with_distance AS (
    SELECT 
        id,
        icao24, 
        firstSeen, 
        estDepartureAirport, 
        lastSeen, 
        estArrivalAirport, 
        callsign,
        estDepartureAirportHorizDistance, 
        estDepartureAirportVertDistance, 
        estArrivalAirportHorizDistance, 
        estArrivalAirportVertDistance,
        
        -- Calculate approximate total distance in km
        -- Using horizontal distances to departure and arrival airports
        -- This is a simplified approximation
        CASE 
            WHEN estDepartureAirportHorizDistance IS NOT NULL 
                AND estArrivalAirportHorizDistance IS NOT NULL
            THEN (estDepartureAirportHorizDistance + estArrivalAirportHorizDistance) / 1000.0
            ELSE NULL
        END AS total_distance_km
    FROM 
        flight_data
    {% if is_incremental %}
    WHERE 
        lastSeen > {{last_incremental_value}}
    {% endif %}
)

SELECT 
    *
FROM 
    flight_data_with_distance
ORDER BY 
    lastSeen ASC