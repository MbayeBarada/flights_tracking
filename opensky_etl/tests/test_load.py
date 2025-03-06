"""
Unit tests for the load module.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd

from  load import load_data_to_db, create_or_replace_view, create_summary_views
from connections.postgresql import FlightData

class TestLoad(unittest.TestCase):
    """Test cases for the load module."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a sample DataFrame
        self.df = pd.DataFrame({
            'icao24': ['abc123', 'def456'],
            'firstSeen': [1614556800, 1614557800],
            'lastSeen': [1614567600, 1614568600],
            'estDepartureAirport': ['EDDF', 'LFPG'],
            'estArrivalAirport': ['LFPG', 'EDDF'],
            'callsign': ['DLH123', 'AFR456'],
            'estDepartureAirportHorizDistance': [1000, 1500],
            'estDepartureAirportVertDistance': [500, 600],
            'estArrivalAirportHorizDistance': [1200, 1300],
            'estArrivalAirportVertDistance': [600, 700],
            'departureAirportCandidatesCount': [1, 1],
            'arrivalAirportCandidatesCount': [1, 1],
            'flight_duration_minutes': [180.0, 160.0],
            'total_distance_km': [2.2, 2.8],
            'airport_pair': ['EDDF-LFPG', 'LFPG-EDDF']
        })
        
        # Create mock engine and session
        self.mock_engine = MagicMock()
        self.mock_session = MagicMock()
    
    @patch('opensky_etl.load.FlightData')
    def test_load_data_to_db(self, mock_flight_data_class):
        """Test loading data to database."""
        # Set up mock FlightData instances
        mock_flight_data_instances = [MagicMock(), MagicMock()]
        mock_flight_data_class.side_effect = mock_flight_data_instances
        
        # Call the function
        result = load_data_to_db(self.df, self.mock_engine, self.mock_session)
        
        # Assertions
        self.assertEqual(result, 2)  # 2 records processed
        self.mock_session.bulk_save_objects.assert_called_once()
        self.mock_session.commit.assert_called_once()
    
    @patch('opensky_etl.load.FlightData')
    def test_load_data_to_db_empty_df(self, mock_flight_data_class):
        """Test loading empty DataFrame."""
        # Call the function with empty DataFrame
        result = load_data_to_db(pd.DataFrame(), self.mock_engine, self.mock_session)
        
        # Assertions
        self.assertEqual(result, 0)  # 0 records processed
        self.mock_session.bulk_save_objects.assert_not_called()
        self.mock_session.commit.assert_not_called()
    
    @patch('opensky_etl.load.FlightData')
    def test_load_data_to_db_error(self, mock_flight_data_class):
        """Test handling errors during database load."""
        # Set up mock to raise an exception during bulk_save_objects
        self.mock_session.bulk_save_objects.side_effect = Exception("Database error")
        
        # Call the function
        result = load_data_to_db(self.df, self.mock_engine, self.mock_session)
        
        # Assertions
        self.assertEqual(result, 0)  # 0 records processed due to error
        self.mock_session.rollback.assert_called_once()
    
    def test_create_or_replace_view(self):
        """Test creating or replacing a database view."""
        # Set up mock connection
        mock_conn = MagicMock()
        self.mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Call the function
        result = create_or_replace_view(
            self.mock_engine, 
            "test_view", 
            "SELECT * FROM flight_data"
        )
        
        # Assertions
        self.assertTrue(result)
        mock_conn.execute.assert_any_call("DROP VIEW IF EXISTS test_view")
        mock_conn.execute.assert_any_call("CREATE VIEW test_view AS SELECT * FROM flight_data")
    
    def test_create_or_replace_view_error(self):
        """Test handling errors when creating a view."""
        # Set up mock to raise an exception during execute
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database error")
        self.mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Call the function
        result = create_or_replace_view(
            self.mock_engine, 
            "test_view", 
            "SELECT * FROM flight_data"
        )
        
        # Assertions
        self.assertFalse(result)
    
    @patch('opensky_etl.load.create_or_replace_view')
    def test_create_summary_views(self, mock_create_view):
        """Test creating summary views."""
        # Set up mock to return True
        mock_create_view.return_value = True
        
        # Call the function
        result = create_summary_views(self.mock_engine)
        
        # Assertions
        self.assertTrue(result)
        self.assertEqual(mock_create_view.call_count, 2)  # Two views should be created

if __name__ == '__main__':
    unittest.main()