"""
Unit tests for the extract module.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, timezone

from extract import extract_flight_data, extract_incremental_data

class TestExtract(unittest.TestCase):
    """Test cases for the extract module."""
    
    @patch('opensky_etl.extract.requests.get')
    def test_extract_flight_data(self, mock_get):
        """Test extracting flight data from OpenSky API."""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "icao24": "abc123",
                "firstSeen": 1614556800,
                "estDepartureAirport": "EDDF",
                "lastSeen": 1614567600,
                "estArrivalAirport": "LFPG",
                "callsign": "DLH123",
                "estDepartureAirportHorizDistance": 1000,
                "estDepartureAirportVertDistance": 500,
                "estArrivalAirportHorizDistance": 1200,
                "estArrivalAirportVertDistance": 600,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 1
            }
        ]
        mock_get.return_value = mock_response
        
        # Call the function
        df = extract_flight_data(
            start_time=1614556800,
            end_time=1614567600
        )
        
        # Assertions
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['icao24'], 'abc123')
        self.assertEqual(df.iloc[0]['callsign'], 'DLH123')
        
        # Verify mock was called correctly
        mock_get.assert_called_with(
            'https://opensky-network.org/api/flights/all?begin=1614556800&end=1614567600',
            auth=(unittest.mock.ANY, unittest.mock.ANY)
        )
    
    @patch('opensky_etl.extract.extract_flight_data')
    def test_extract_incremental_data(self, mock_extract):
        """Test extracting incremental flight data."""
        # Mock extract_flight_data
        mock_df = pd.DataFrame({
            'icao24': ['abc123'],
            'firstSeen': [1614556800],
            'lastSeen': [1614567600]
        })
        mock_extract.return_value = mock_df
        
        # Call the function
        last_value = 1614556800
        df = extract_incremental_data(last_value)
        
        # Assertions
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        
        # Verify mock was called correctly
        mock_extract.assert_called_with(start_time=last_value)
    
    @patch('opensky_etl.extract.requests.get')
    def test_extract_flight_data_api_error(self, mock_get):
        """Test handling API errors during extraction."""
        # Mock the API response with an error
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_get.return_value = mock_response
        
        # Call the function
        df = extract_flight_data(
            start_time=1614556800,
            end_time=1614567600
        )
        
        # Assertions
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

if __name__ == '__main__':
    unittest.main()