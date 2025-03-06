"""
Unit tests for the flight data pipeline.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from pipelines.flight_data_pipeline import FlightDataPipeline

class TestFlightDataPipeline(unittest.TestCase):
    """Test cases for the flight data pipeline."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create sample data
        self.sample_df = pd.DataFrame({
            'icao24': ['abc123', 'def456'],
            'firstSeen': [1614556800, 1614557800],
            'lastSeen': [1614567600, 1614568600],
            'estDepartureAirport': ['EDDF', 'LFPG'],
            'estArrivalAirport': ['LFPG', 'EDDF'],
            'callsign': ['DLH123', 'AFR456']
        })
    
    @patch('opensky_etl.pipelines.flight_data_pipeline.get_db_connection')
    @patch('opensky_etl.pipelines.flight_data_pipeline.get_last_incremental_value')
    @patch('opensky_etl.pipelines.flight_data_pipeline.extract_flight_data')
    @patch('opensky_etl.pipelines.flight_data_pipeline.transform_flight_data')
    @patch('opensky_etl.pipelines.flight_data_pipeline.load_data_to_db')
    @patch('opensky_etl.pipelines.flight_data_pipeline.create_summary_views')
    def test_pipeline_full_load(
        self, mock_create_views, mock_load, mock_transform, 
        mock_extract, mock_get_last_value, mock_get_db
    ):
        """Test pipeline with full load."""
        # Set up mocks
        mock_engine = MagicMock()
        mock_session = MagicMock()
        mock_metadata = MagicMock()
        mock_get_db.return_value = (mock_engine, mock_session, mock_metadata)
        
        mock_extract.return_value = self.sample_df
        mock_transform.return_value = self.sample_df
        mock_load.return_value = 2  # 2 records processed
        mock_create_views.return_value = True
        
        # Create and run pipeline with force_full_load=True
        pipeline = FlightDataPipeline()
        result = pipeline.run(force_full_load=True)
        
        # Assertions
        self.assertTrue(result)
        mock_extract.assert_called_once()
        mock_transform.assert_called_once()
        mock_load.assert_called_once()
        mock_create_views.assert_called_once()
        
        # Verify pipeline stats
        stats = pipeline.get_stats()
        self.assertEqual(stats['records_processed'], 2)
        self.assertFalse(stats['is_incremental'])
    
    @patch('opensky_etl.pipelines.flight_data_pipeline.get_db_connection')
    @patch('opensky_etl.pipelines.flight_data_pipeline.get_last_incremental_value')
    @patch('opensky_etl.pipelines.flight_data_pipeline.extract_incremental_data')
    @patch('opensky_etl.pipelines.flight_data_pipeline.transform_flight_data')
    @patch('opensky_etl.pipelines.flight_data_pipeline.load_data_to_db')
    @patch('opensky_etl.pipelines.flight_data_pipeline.create_summary_views')
    def test_pipeline_incremental_load(
        self, mock_create_views, mock_load, mock_transform, 
        mock_extract_incremental, mock_get_last_value, mock_get_db
    ):
        """Test pipeline with incremental load."""
        # Set up mocks
        mock_engine = MagicMock()
        mock_session = MagicMock()
        mock_metadata = MagicMock()
        mock_get_db.return_value = (mock_engine, mock_session, mock_metadata)
        
        # Set last incremental value
        mock_get_last_value.return_value = 1614567600
        
        mock_extract_incremental.return_value = self.sample_df
        mock_transform.return_value = self.sample_df
        mock_load.return_value = 2  # 2 records processed
        mock_create_views.return_value = True
        
        # Create and run pipeline with incremental load
        pipeline = FlightDataPipeline()
        result = pipeline.run(force_full_load=False)
        
        # Assertions
        self.assertTrue(result)
        mock_extract_incremental.assert_called_once_with(1614567600)
        mock_transform.assert_called_once()
        mock_load.assert_called_once()
        mock_create_views.assert_called_once()
        
        # Verify pipeline stats
        stats = pipeline.get_stats()
        self.assertEqual(stats['records_processed'], 2)
        self.assertTrue(stats['is_incremental'])
        self.assertEqual(stats['last_incremental_value'], 1614567600)
    
    @patch('opensky_etl.pipelines.flight_data_pipeline.get_db_connection')
    @patch('opensky_etl.pipelines.flight_data_pipeline.get_last_incremental_value')
    @patch('opensky_etl.pipelines.flight_data_pipeline.extract_flight_data')
    def test_pipeline_no_data(
        self, mock_extract, mock_get_last_value, mock_get_db
    ):
        """Test pipeline when no data is extracted."""
        # Set up mocks
        mock_engine = MagicMock()
        mock_session = MagicMock()
        mock_metadata = MagicMock()
        mock_get_db.return_value = (mock_engine, mock_session, mock_metadata)
        
        # Return empty DataFrame
        mock_extract.return_value = pd.DataFrame()
        
        # Create and run pipeline
        pipeline = FlightDataPipeline()
        result = pipeline.run()
        
        # Assertions
        self.assertFalse(result)
        
        # Verify pipeline stats
        stats = pipeline.get_stats()
        self.assertEqual(stats['records_processed'], 0)

if __name__ == '__main__':
    unittest.main()