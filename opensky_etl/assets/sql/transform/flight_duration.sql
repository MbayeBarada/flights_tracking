-- Calculate flight duration in minutes
-- This transformation uses firstSeen and lastSeen timestamps

WITH flight_duration_calc AS (
    SELECT 
        id,
        icao24, 
        firstSeen, 
        estDepartureAirport, 
        lastSeen, 
        estArrivalAirport, 
        callsign,
        
        -- Calculate flight duration in minutes
        CASE 
            WHEN firstSeen IS NOT NULL AND lastSeen IS NOT NULL
            THEN (lastSeen - firstSeen) / 60.0  -- Convert seconds to minutes
            ELSE NULL
        END AS flight_duration_minutes
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
    flight_duration_calc
ORDER BY 
    lastSeen ASC