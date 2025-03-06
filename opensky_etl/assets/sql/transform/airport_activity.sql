-- Analyze airport activity and create airport pairs
-- This transformation creates an airport pair for each flight

WITH airport_pairs AS (
    SELECT 
        id,
        icao24, 
        firstSeen, 
        estDepartureAirport, 
        lastSeen, 
        estArrivalAirport, 
        callsign,
        
        -- Create airport pair identifier
        CASE 
            WHEN estDepartureAirport IS NOT NULL AND estArrivalAirport IS NOT NULL
            THEN CONCAT(estDepartureAirport, '-', estArrivalAirport)
            ELSE NULL
        END AS airport_pair
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
    airport_pairs
ORDER BY 
    lastSeen ASC