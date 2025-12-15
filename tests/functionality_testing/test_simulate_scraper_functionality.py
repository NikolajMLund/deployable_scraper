import os
import pytest
from unittest.mock import patch, MagicMock
import time
import json
from scraper_schedule import run_scraper_schedule        
from helper_class import tdb as db

###############################################################################################################################
# Fixtures: 
###############################################################################################################################
@pytest.fixture(scope='session')
def tdb():
    """Create and setup test database instance"""
    tdb = db(name='./test')

    if tdb.check_if_db_exists():
        raise FileExistsError(f'To run tests first delete the {tdb.name}.db in the root directory')

    tdb.create_db()
    
    yield tdb

    tdb.clean_up_db()

@pytest.fixture
def mock_db_with_tdb(monkeypatch, tdb): 
    monkeypatch.setattr('scraper_schedule.db', lambda name=None, **kwargs: tdb)
    return tdb

@pytest.fixture
def env_locations_once(monkeypatch):
    """Setup environment for locations scraper in once mode"""
    monkeypatch.setenv('RUN_MODE', 'once')
    monkeypatch.setenv('LOG_LEVEL', 'INFO')
    monkeypatch.setenv('SCRAPER_TYPE', 'locations')
    monkeypatch.setenv('MAX_WORKERS', '1')
    monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
    #monkeypatch.setenv('DB_PATHNAME', './test')

@pytest.fixture
def env_locations_scheduled(monkeypatch):
    """Setup environment for locations scraper in scheduled mode"""
    monkeypatch.setenv('RUN_MODE', 'scheduled')
    monkeypatch.setenv('LOG_LEVEL', 'INFO')
    monkeypatch.setenv('SCRAPER_TYPE', 'locations')
    monkeypatch.setenv('MINUTE_INTERVAL', '1')
    monkeypatch.setenv('MAX_WORKERS', '1')
    monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
    #monkeypatch.setenv('DB_PATHNAME', './test')

@pytest.fixture
def env_standard_once(monkeypatch):
    """Setup environment for standard scraper in once mode"""
    monkeypatch.setenv('RUN_MODE', 'once')
    monkeypatch.setenv('LOG_LEVEL', 'INFO')
    monkeypatch.setenv('SCRAPER_TYPE', 'standard')
    monkeypatch.setenv('MAX_WORKERS', '1')
    monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
    #monkeypatch.setenv('DB_PATHNAME', './test')

@pytest.fixture
def env_standard_scheduled(monkeypatch):
    """Setup environment for standard scraper in scheduled mode"""
    monkeypatch.setenv('RUN_MODE', 'scheduled')
    monkeypatch.setenv('LOG_LEVEL', 'INFO')
    monkeypatch.setenv('SCRAPER_TYPE', 'standard')
    monkeypatch.setenv('MINUTE_INTERVAL', '1')
    monkeypatch.setenv('MAX_WORKERS', '1')
    monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
    #monkeypatch.setenv('DB_PATHNAME', './test')

@pytest.fixture
def env_prices_once(monkeypatch):
    """Setup environment for standard scraper in once mode"""
    monkeypatch.setenv('RUN_MODE', 'once')
    monkeypatch.setenv('LOG_LEVEL', 'INFO')
    monkeypatch.setenv('SCRAPER_TYPE', 'prices')
    monkeypatch.setenv('MAX_WORKERS', '1')
    monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
    #monkeypatch.setenv('DB_PATHNAME', './test')

@pytest.fixture
def env_prices_scheduled(monkeypatch):
    """Setup environment for standard scraper in scheduled mode"""
    monkeypatch.setenv('RUN_MODE', 'scheduled')
    monkeypatch.setenv('LOG_LEVEL', 'INFO')
    monkeypatch.setenv('SCRAPER_TYPE', 'prices')
    monkeypatch.setenv('MINUTE_INTERVAL', '1')
    monkeypatch.setenv('MAX_WORKERS', '1')
    monkeypatch.setenv('SLEEP_IN_SECONDS', '0.1')
    #monkeypatch.setenv('DB_PATHNAME', './test')

@pytest.fixture
def mock_location_scraper(monkeypatch):
    """Mock the location scraper's run() method"""
    with patch('scraper_schedule.loc_scraper') as mock_scraper_class:
        # Create a mock instance
        mock_instance = MagicMock()
        
        # Mock the results attribute with fake location data
        test_data_path = os.path.join('tests', 'data', 'scrape_simulation', 'locations.json')
        with open(test_data_path, 'r', encoding='utf-8') as f:
            test_data = json.load(f)
        
        # mock identifiers
        mock_instance.identifiers = ['locations']
        
        # Make run() method do nothing but set the results
        def mock_run(max_workers):
            mock_instance.results = test_data
        
        mock_instance.run = MagicMock(side_effect=mock_run)
        
        # Make the scraper class return this mock instance when instantiated
        mock_scraper_class.return_value = mock_instance
        
        yield mock_instance
    
@pytest.fixture
def mock_avail_scraper(monkeypatch):
    """Mock the availability scraper's run() method"""
    with patch('scraper_schedule.avail_scraper') as mock_scraper_class:
        # Create a mock instance
        mock_instance = MagicMock()
        
        # Mock the results attribute with fake availability data
        # Load test data from file
        test_data_path = os.path.join('tests', 'data', 'scrape_simulation', 'availability.json')
        with open(test_data_path, 'r', encoding='utf-8') as f:
            test_data = json.load(f)
        
        mock_instance.identifiers = list(test_data.keys())
        # Make run() method do nothing but set the results
        def mock_run(max_workers):
            mock_instance.results = test_data

        mock_instance.run = MagicMock(side_effect=mock_run)

        # Make the scraper class return this mock instance when instantiated
        mock_scraper_class.return_value = mock_instance
        
        yield mock_instance

@pytest.fixture
def mock_price_scraper(monkeypatch):
    """Mock the prices scraper's run() method"""
    with patch('scraper_schedule.price_scraper') as mock_scraper_class:
        # Create a mock instance
        mock_instance = MagicMock()
        
        # Mock the results attribute with fake prices data
        # Load test data from file
        test_data_path = os.path.join('.','tests', 'data', 'scrape_simulation', 'prices.json')
        with open(test_data_path, 'r', encoding='utf-8') as f:
            test_data = json.load(f)
        
        mock_instance.identifiers = list(test_data.keys())
        # Make run() method do nothing but set the results
        def mock_run(max_workers):
            mock_instance.results = test_data

        mock_instance.run = MagicMock(side_effect=mock_run)

        # Make the scraper class return this mock instance when instantiated
        mock_scraper_class.return_value = mock_instance
        
        yield mock_instance

###############################################################################################################################
# TESTS: Currently only the 'once' environment
###############################################################################################################################

def test_run_locations_once(
        env_locations_once, 
        mock_location_scraper,
        mock_avail_scraper,
        mock_price_scraper,
        tdb,
        mock_db_with_tdb,
    ):
    run_scraper_schedule()
    

def test_run_avail_once(
        env_standard_once, 
        mock_location_scraper,
        mock_avail_scraper,
        mock_price_scraper,
        tdb,
        mock_db_with_tdb,
    ):
    run_scraper_schedule()

def test_run_prices_once(
        env_prices_once, 
        mock_location_scraper,
        mock_avail_scraper,
        mock_price_scraper,
        tdb,
        mock_db_with_tdb,
    ):
    run_scraper_schedule()


def test_run_locations_scheduled(
        env_locations_scheduled, 
        mock_location_scraper,
        mock_avail_scraper,
        mock_price_scraper,
        tdb,
        mock_db_with_tdb,
    ):
    from apscheduler.schedulers.background import BackgroundScheduler
    sch = run_scraper_schedule(scheduler_class=BackgroundScheduler)
    # Verify scheduler was created
    assert sch is not None
    jobs = sch.get_jobs()
    assert len(jobs) == 1, 'no job was scheduled'
    assert jobs[0].id == 'locations_scraper', f"a job was scheduled but with name {jobs[0].id} and not 'Availability_scraper"
    # Let it run briefly
    time.sleep(2)
    # Stop scheduler
    sch.shutdown(wait=True)
    mock_location_scraper.run.assert_called_once()

def test_run_avail_scheduled(
        env_standard_scheduled, 
        mock_location_scraper,
        mock_avail_scraper,
        mock_price_scraper,
        tdb,
        mock_db_with_tdb,
    ):
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
    sch.shutdown(wait=True)
    mock_location_scraper.run.assert_called_once()
    mock_avail_scraper.run.assert_called_once()

def test_run_prices_scheduled(
        env_prices_scheduled, 
        mock_location_scraper,
        mock_avail_scraper,
        mock_price_scraper,
        tdb,
        mock_db_with_tdb,
    ):
    from apscheduler.schedulers.background import BackgroundScheduler
    sch = run_scraper_schedule(scheduler_class=BackgroundScheduler)
    # Verify scheduler was created
    assert sch is not None
    jobs = sch.get_jobs()
    assert len(jobs) == 1, 'no job was scheduled'
    assert jobs[0].id == 'Prices_scraper', f"a job was scheduled but with name {jobs[0].id} and not 'Availability_scraper"
    # Let it run briefly
    time.sleep(2)
    # Stop scheduler
    sch.shutdown(wait=True)
    mock_location_scraper.run.assert_called_once()
    mock_price_scraper.run.assert_called_once()

# def test_run_scraper_as_main_once(
#         env_locations_once, 
#         mock_location_scraper,
#         mock_avail_scraper,
#         mock_price_scraper,
#         tdb,
#         mock_db_with_tdb,
#     ):
#     import subprocess
#     import sys
#     results=subprocess.run(
#         [sys.executable, './src/main_scripts/run_scraper_schedule.py'],
#         env=env_locations_once,
#         capture_output=True,
#         text=True,
#         timeout=30
#     )

#     breakpoint()    
