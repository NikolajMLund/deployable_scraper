import os
import sys

import pytest
from unittest.mock import patch, MagicMock
import time

            
class TestScraperSchedule:    
    @pytest.fixture
    def env_fast_once(self, monkeypatch):
        """Setup environment for fast scraper in once mode"""
        monkeypatch.setenv('RUN_MODE', 'once')
        monkeypatch.setenv('LOG_LEVEL', 'INFO')
        monkeypatch.setenv('SCRAPER_TYPE', 'fast')
        monkeypatch.setenv('MAX_WORKERS', '1')
        monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
        monkeypatch.setenv('DB_PATHNAME', './data/db/charging_fresh')
        
    
    @pytest.fixture
    def env_fast_scheduled(self, monkeypatch):
        """Setup environment for fast scraper in scheduled mode"""
        monkeypatch.setenv('RUN_MODE', 'scheduled')
        monkeypatch.setenv('LOG_LEVEL', 'INFO')
        monkeypatch.setenv('SCRAPER_TYPE', 'fast')
        monkeypatch.setenv('MINUTE_INTERVAL', '1')
        monkeypatch.setenv('MAX_WORKERS', '1')
        monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
        monkeypatch.setenv('DB_PATHNAME', './data/db/charging_fresh')

    @pytest.fixture
    def env_prices_once(self, monkeypatch):
        """Setup environment for fast scraper in once mode"""
        monkeypatch.setenv('RUN_MODE', 'once')
        monkeypatch.setenv('LOG_LEVEL', 'INFO')
        monkeypatch.setenv('SCRAPER_TYPE', 'Prices')
        monkeypatch.setenv('MAX_WORKERS', '1')
        monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
        monkeypatch.setenv('DB_PATHNAME', './data/db/charging_fresh')

    @pytest.fixture
    def env_prices_scheduled(self, monkeypatch):
        """Setup environment for fast scraper in once mode"""
        monkeypatch.setenv('RUN_MODE', 'once')
        monkeypatch.setenv('LOG_LEVEL', 'INFO')
        monkeypatch.setenv('SCRAPER_TYPE', 'Prices')
        monkeypatch.setenv('MAX_WORKERS', '1')
        monkeypatch.setenv('MINUTE_INTERVAL', '1')
        monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
        monkeypatch.setenv('DB_PATHNAME', './data/db/charging_fresh')

    @pytest.fixture
    def env_prices_once_on_existing_data(self, monkeypatch):
        """Setup environment for fast scraper in once mode"""
        monkeypatch.setenv('RUN_MODE', 'once')
        monkeypatch.setenv('LOG_LEVEL', 'INFO')
        monkeypatch.setenv('SCRAPER_TYPE', 'Prices')
        monkeypatch.setenv('MAX_WORKERS', '1')
        monkeypatch.setenv('MINUTE_INTERVAL', '1')
        monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
        monkeypatch.setenv('DB_PATHNAME', './data/db/charging_existing')

    def test_scraper_once_on_existing_data(self, env_prices_once_on_existing_data,):
        """Test Prices scraper runs once - Actual web scraping"""
        assert os.environ['SCRAPER_TYPE'] == 'Prices'
        assert os.environ['RUN_MODE'] == 'once'

        from scraper_schedule import run_scraper_schedule        
        #HELP: first delete existing file on './data/db/charging_existing.db'
        # if os.path.exists(os.environ.get('DB_PATHNAME')):
        #     os.remove(os.environ.get('DB_PATHNAME'))

        #HELP: USE bat script command './data/db/charging_existing' 'sqlite3_rsync nikol@raspberrypi:deployable_scraper/data/db/charging.db ./data/db/charging_existing.db'
        
        run_scraper_schedule()

        #HELP: Then do clean up:
        # if os.path.exists(os.environ.get('DB_PATHNAME')):
        #     os.remove(os.environ.get('DB_PATHNAME'))

    def test_scraper_once(self, env_prices_once,):
        """Test Prices scraper runs once - Actual web scraping"""
        assert os.environ['SCRAPER_TYPE'] == 'Prices'
        assert os.environ['RUN_MODE'] == 'once'

        from scraper_schedule import run_scraper_schedule        
        run_scraper_schedule()
    
    # def test_scraper_scheduled(self, env_prices_scheduled,):
    #     """Test fast scraper in scheduled mode - mocked run() methods"""
    #     assert os.environ['SCRAPER_TYPE'] == 'fast'
    #     assert os.environ['RUN_MODE'] == 'scheduled'

    #     from scraper_schedule import run_scraper_schedule
    #     from apscheduler.schedulers.background import BackgroundScheduler
        
    #     sch = run_scraper_schedule(scheduler_class=BackgroundScheduler)
        
    #     # Verify scheduler was created
    #     assert sch is not None
    #     jobs = sch.get_jobs()
    #     assert len(jobs) == 1, 'no job was scheduled'
    #     assert jobs[0].id == 'Availability_scraper', f"a job was scheduled but with name {jobs[0].id} and not 'Availability_scraper"
        
    #     # Let it run briefly
    #     time.sleep(2)
        
    #     # Stop scheduler
    #     sch.shutdown(wait=False)
        
    #     mock_loc_scraper.run.assert_called_once()
    #     mock_avail_scraper.run.assert_called_once() 
        
    


