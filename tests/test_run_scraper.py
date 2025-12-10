import os
import pytest
from unittest.mock import patch, MagicMock
import time
            
class TestScraperSchedule:
    
    # @pytest.fixture(autouse=True)
    # def clean_imports(self):
    #     """Clean up imported modules before each test to avoid caching issues"""
    #     import sys
    #     # Remove cached imports that might interfere with mocking
    #     modules_to_clean = [
    #         'main_scripts.run_scraper_schedule',
    #         'scrapers.with_requests.scrape_availability_with_api',
    #         'scrapers.with_requests.scrape_locations_with_api',
    #     ]
    #     for module in modules_to_clean:
    #         if module in sys.modules:
    #             del sys.modules[module]
    #     yield
    
    @pytest.fixture
    def env_fast_once(self, monkeypatch):
        """Setup environment for fast scraper in once mode"""
        monkeypatch.setenv('RUN_MODE', 'once')
        monkeypatch.setenv('LOG_LEVEL', 'INFO')
        monkeypatch.setenv('SCRAPER_TYPE', 'fast')
        monkeypatch.setenv('MAX_WORKERS', '1')
        monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
    
    @pytest.fixture
    def env_fast_scheduled(self, monkeypatch):
        """Setup environment for fast scraper in scheduled mode"""
        monkeypatch.setenv('RUN_MODE', 'scheduled')
        monkeypatch.setenv('LOG_LEVEL', 'INFO')
        monkeypatch.setenv('SCRAPER_TYPE', 'fast')
        monkeypatch.setenv('MINUTE_INTERVAL', '1')
        monkeypatch.setenv('MAX_WORKERS', '1')
        monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
    
    @pytest.fixture
    def mock_avail_scraper(self):
        """Mock the availability scraper's run() method"""
        with patch('main_scripts.run_scraper_schedule.avail_scraper') as mock_scraper_class:
            # Create a mock instance
            mock_instance = MagicMock()
            
            # Mock the results attribute with fake availability data
            mock_instance.results = {
                "loc123": {
                    'data': {
                        "state": "Active",
                        "evses": {
                            "evse123": {
                                "connectors": {
                                    "evse123-2": {
                                        "plugType": "Type2",
                                        "powerType": "AC3Phase",
                                        "evseConnectorId": "evse123-2",
                                        "maxPowerKw": 22.08,
                                        "connectorId": 2,
                                        "speed": "Standard"
                                    }
                                },
                                "evseId": "evse123",
                                "chargePointId": "112277",
                                "vendorName": "Compleo CS AG"
                            },
                        },
                        "locationId": "loc123",
                        "availability": {
                            "evses": {
                                "evse123": {
                                    "evseId": "evse123",
                                    "geohash": "u3b54qt46e",
                                    "locationId": "loc123",
                                    "status": "Occupied",
                                    "timestamp": "2025-10-15T15:07:10.2823816+00:00"
                                },
                            },
                            "geohash": "u3b54qt46e"
                        }
                    }
                }
            }            
            # Make the scraper class return this mock instance when instantiated
            mock_scraper_class.return_value = mock_instance
            
            yield mock_instance
    
    @pytest.fixture
    def mock_loc_scraper(self):
        """Mock the locations scraper's run() method"""
        with patch('main_scripts.run_scraper_schedule.loc_scraper') as mock_scraper_class:
            # Create a mock instance
            mock_instance = MagicMock()
            
            mock_instance.results={
                "locations": {
                    "loc123": {
                        "timestamp": {
                            "seconds": 1759276923,
                            "nanoseconds": 775725000
                        },
                        "connectorCounts": [
                            {
                                "count": 4,
                                "plugType": "Type2",
                                "speed": "Standard"
                            }
                        ],
                        "locationId": "loc123",
                        "origin": "Bla",
                        "coordinates": {
                            "geohash": "65757",
                            "lng": 12.311272,
                            "quality": "Unknown",
                            "altitude": '0',
                            "lat": 55.923953
                        },
                    }
                }         
            }   
            # Mock the results attribute
            mock_instance.identifiers = ['locations']
            
            # Make the scraper class return this mock instance
            mock_scraper_class.return_value = mock_instance
            
            yield mock_instance
    
    def test_scraper_once(self, env_fast_once, mock_avail_scraper, mock_loc_scraper):
        """Test fast scraper runs once - mocked run() methods"""
        assert os.environ['SCRAPER_TYPE'] == 'fast'
        assert os.environ['RUN_MODE'] == 'once'

        from main_scripts.run_scraper_schedule import run_scraper_schedule        
        run_scraper_schedule()
        
        # Verify the scrapers' run() methods were called
        mock_loc_scraper.run.assert_called_once()
        mock_avail_scraper.run.assert_called_once()
        

    
    def test_scraper_scheduled(self, env_fast_scheduled, mock_avail_scraper, mock_loc_scraper):
        """Test fast scraper in scheduled mode - mocked run() methods"""
        assert os.environ['SCRAPER_TYPE'] == 'fast'
        assert os.environ['RUN_MODE'] == 'scheduled'

        from main_scripts.run_scraper_schedule import run_scraper_schedule
        from apscheduler.schedulers.background import BackgroundScheduler
        
        sch = run_scraper_schedule(scheduler_class=BackgroundScheduler)
        
        # Verify scheduler was created
        assert sch is not None
        jobs = sch.get_jobs()
        assert len(jobs) == 1, 'no job was scheduled'
        assert jobs[0].id == 'Availability_scraper', f"a job was scheduled but with name {jobs[0].id} and not 'Availability_scraper"
        
        # Let it run briefly
        time.sleep(2)
        
        # Stop scheduler
        sch.shutdown(wait=False)
        
        mock_loc_scraper.run.assert_called_once()
        mock_avail_scraper.run.assert_called_once() 
        
    


