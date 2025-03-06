-- Extract flight data from the flight_data table
-- This SQL uses Jinja2-style templating with {{variable}} syntax

{% if is_incremental %}
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
    departureAirportCandidatesCount, 
    arrivalAirportCandidatesCount
FROM 
    flight_data
WHERE 
    lastSeen > {{last_incremental_value}}
ORDER BY 
    lastSeen ASC
{% else %}
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
    departureAirportCandidatesCount, 
    arrivalAirportCandidatesCount
FROM 
    flight_data
ORDER BY 
    lastSeen ASC
{% endif %}