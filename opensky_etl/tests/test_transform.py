"""
Unit tests for the transform module.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import tempfile

from transform import (
    apply_sql_transformation, 
    transform_flight_data,
    calculate_flight_duration,
    calculate_flight_distance,
    create_airport_pairs
)

class TestTransform(unittest.TestCase):
    """Test cases for the transform module."""
    
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
            'arrivalAirportCandidatesCount': [1, 1]
        })
        
        # Create a mock engine
        self.mock_engine = MagicMock()
    
    def test_calculate_flight_duration(self):
        """Test calculating flight duration."""
        # Call the function
        result_df = calculate_flight_duration(self.df)
        
        # Assertions
        self.assertIn('flight_duration_minutes', result_df.columns)
        self.assertEqual(result_df.iloc[0]['flight_duration_minutes'], (1614567600 - 1614556800) / 60.0)
        self.assertEqual(result_df.iloc[1]['flight_duration_minutes'], (1614568600 - 1614557800) / 60.0)
    
    def test_calculate_flight_distance(self):
        """Test calculating flight distance."""
        # Call the function
        result_df = calculate_flight_distance(self.df)
        
        # Assertions
        self.assertIn('total_distance_km', result_df.columns)
        self.assertEqual(result_df.iloc[0]['total_distance_km'], (1000 + 1200) / 1000.0)
        self.assertEqual(result_df.iloc[1]['total_distance_km'], (1500 + 1300) / 1000.0)
    
    def test_create_airport_pairs(self):
        """Test creating airport pairs."""
        # Call the function
        result_df = create_airport_pairs(self.df)
        
        # Assertions
        self.assertIn('airport_pair', result_df.columns)
        self.assertEqual(result_df.iloc[0]['airport_pair'], 'EDDF-LFPG')
        self.assertEqual(result_df.iloc[1]['airport_pair'], 'LFPG-EDDF')
    
    @patch('opensky_etl.transform.apply_sql_transformation')
    def test_transform_flight_data(self, mock_apply_sql):
        """Test transforming flight data."""
        # Mock SQL transformation responses
        mock_duration_df = pd.DataFrame({
            'id': [1, 2],
            'flight_duration_minutes': [180.0, 160.0]
        })
        
        mock_distance_df = pd.DataFrame({
            'id': [1, 2],
            'total_distance_km': [2.2, 2.8]
        })
        
        mock_airport_df = pd.DataFrame({
            'id': [1, 2],
            'airport_pair': ['EDDF-LFPG', 'LFPG-EDDF']
        })
        
        # Set up mock to return different DataFrames based on SQL file
        def side_effect(engine, sql_path, is_incremental, last_value):
            if 'flight_duration.sql' in sql_path:
                return mock_duration_df
            elif 'flight_distance.sql' in sql_path:
                return mock_distance_df
            elif 'airport_activity.sql' in sql_path:
                return mock_airport_df
            return pd.DataFrame()
        
        mock_apply_sql.side_effect = side_effect
        
        # Call the function
        result_df = transform_flight_data(self.df, self.mock_engine)
        
        # Assertions
        self.assertIn('flight_duration_minutes', result_df.columns)
        self.assertIn('total_distance_km', result_df.columns)
        self.assertIn('airport_pair', result_df.columns)
    
    def test_apply_sql_transformation(self):
        """Test applying SQL transformations from file."""
        # Create a temporary SQL file with Jinja2-style templating
        sql_content = """
        -- Test SQL file
        {% if is_incremental %}
        SELECT * FROM flight_data WHERE lastSeen > {{last_incremental_value}}
        {% endif %}
        SELECT * FROM flight_data
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
            temp_file.write(sql_content)
            temp_file_path = temp_file.name
        
        try:
            # Mock engine's connect return value
            mock_conn = MagicMock()
            self.mock_engine.connect.return_value.__enter__.return_value = mock_conn
            
            # Set up mock to return a DataFrame when pd.read_sql is called
            with patch('opensky_etl.transform.pd.read_sql') as mock_read_sql:
                mock_read_sql.return_value = pd.DataFrame({'test': [1, 2, 3]})
                
                # Test with incremental=True
                result_df = apply_sql_transformation(
                    self.mock_engine, temp_file_path, is_incremental=True, last_value=1000
                )
                
                # Assertions
                self.assertIsInstance(result_df, pd.DataFrame)
                self.assertFalse(result_df.empty)
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)

if __name__ == '__main__':
    unittest.main()