from helper_class import tdb as db
import sqlite3
import json
import pytest

@pytest.fixture
def tdb():
    tdb = db(name='test')

    if tdb.check_if_db_exists():
        raise FileExistsError(f'To run tests first delete the {tdb.name}.db in the root directory')

    tdb.create_db()
    
    yield tdb

    tdb.clean_up_db()

@pytest.fixture
def tdb_sampledata(tdb):
    tdb.insert_location_test_data() 
    tdb.insert_connectorGroup_test_data()
    return tdb

@pytest.fixture
def tdb_mockdata(tdb):
    loc_insert = {
        'locationId': 'ABC',
        'revision': 1,
        'name': 'test',
        'partnerStatus': 'No' ,
        'isRoamingPartner': True,
        'origin': 'something' ,
        'coords_lat': 3.10 ,
        'coords_lng': 1.28 ,
        'ts_seconds':  100,
        'ts_nanoseconds': 200,
    }
    tdb.insert_row('locations', loc_insert)


    ## Inserting connectorCounts into db.
    connector_count_insert = {
        'locationId': 'ABC',
        'revision': 1, 
        'connectorGroup': 1, 
    }
    tdb.insert_row('connectorGroups', connector_count_insert)

    ## Inserting evseids into db.
    evse_insert = {
        'locationId': 'ABC',
        'revision': 1,
        'evseId': '1',
    }
    tdb.insert_row('evseids', evse_insert)

    return tdb


# ============================================================================
# Tests - Each uses the appropriate fixture
# ============================================================================

def test_create_db(tdb):
    assert tdb.check_if_db_exists(), f'{tdb.name}.db was not created'

def test_foreign_keys_on_availabilityAggregated(tdb_mockdata):
    # Populating test db. 
    ## Inserting locations into db.
    ## insert create_availabilityAggregated_table with right parameters
    availability_agg_insert = {
        'locationId': 'ABC',
        'revision': 1, 
        'connectorGroup': 1, 
        'availableCount': 1,
        'totalCount': 2,
        'createdAt': 100
    }
    success, error=tdb_mockdata.insert_row('availabilityAggregated', availability_agg_insert)

    assert success, 'Could not insert correct data into AvailabilityLog'

    ## insert create_availabilityAggregated_table with wrong parameters
    wrong_availability_agg_insert = {
        'locationId': 'ABC',
        'revision': 1, 
        'connectorGroup': 9999, 
        'availableCount': 1,
        'totalCount': 2,
        'createdAt': 100
    }

    success, error=tdb_mockdata.insert_row('availabilityAggregated', wrong_availability_agg_insert)

    assert error.sqlite_errorcode == 787, 'sqlite does not return 787 error when inserting non existing evseId'

def test_foreign_keys_on_availabilityLog(tdb_mockdata):
    ## insert create_availabilityLog_table with right parameters
    availability_log_insert = {
        'locationId': 'ABC',
        'revision': 1,
        'evseId': '1',
        'status': 'Available',
        'timestamp': 100
    }
    success, error=tdb_mockdata.insert_row('availabilityLog', availability_log_insert)

    assert success, 'Could not insert correct data into AvailabilityLog'

    ## insert create_availabilityLog_table with wrong parameters
    wrong_availability_log_insert = {
        'locationId': 'ABC',
        'revision': 1,
        'evseId': '9999',
        # missing connectorId and status
        'timestamp': 100
    }
    success, error=tdb_mockdata.insert_row('availabilityLog', wrong_availability_log_insert)

    assert error.sqlite_errorcode == 787, 'sqlite does not return 787 error when inserting non existing evseId'

def test_foreign_keys_on_connectorGroup(tdb_mockdata):
    ## insert with right parameters
    right_connectorGroup_insert = {
        'locationId': 'ABC',
        'revision': 1,
        'connectorGroup': 1,
        'plugType': 'Type 2',
        'speed': 'Standard',
        'Count': 1,
    }
    success, error=tdb_mockdata.insert_row('connectorGroups', right_connectorGroup_insert)
    
    ## insert with wrong parameters
    wrong_connectorGroup_insert = {
        'locationId': 'ABC',
        'revision': -9999,
        'connectorGroup': 1,
        'plugType': 'Type 2',
        'speed': 'Standard',
        'Count': 1,
    }

    success, error=tdb_mockdata.insert_row('connectorGroups', wrong_connectorGroup_insert)

    assert error.sqlite_errorcode == 787, 'sqlite does not return 787 error when inserting non existing revision'

def test_insert_availabilityLog(tdb_sampledata):
    with open('tests/data/availability_test.json', 'r', encoding='utf-8') as f:
        availability = json.load(f)

    for k in availability.keys():
        v = availability[k]
        tdb_sampledata.insert_row_in_availabilityLog_table(loc_avail_query=v)

    conn = sqlite3.connect(f'{tdb_sampledata.name}.db')
    cursor = conn.cursor()    
    cursor.execute(f"SELECT COUNT(*) FROM availabilityLog")
    count_availabilityLog = cursor.fetchone()[0]
    assert count_availabilityLog == 30, f'there should be 30 entries, but db have {count_availabilityLog}'

    cursor.execute(f"SELECT COUNT(*) FROM evseIds")
    count_evseIds = cursor.fetchone()[0]
    assert count_evseIds == 30, f'there should be 30 entries, but db have {count_evseIds}'

    conn.close()

def test_insert_connectorGroups(tdb_sampledata):
        conn = sqlite3.connect(f'{tdb_sampledata.name}.db')
        cursor = conn.cursor()    
        cursor.execute(f"SELECT COUNT(*) FROM ConnectorGroups")
        count = cursor.fetchone()[0]
        assert count == 3145, f'Inserted rows do not match, entries in JSON test file. is {count} but should be 3145'

        conn.close()
        
def test_insert_locations(tdb_sampledata):
    conn = sqlite3.connect(f'{tdb_sampledata.name}.db')
    cursor = conn.cursor()    
    cursor.execute(f"SELECT COUNT(*) FROM locations")
    count = cursor.fetchone()[0]
    assert count == 2960, f'Inserted rows do not match, entries in JSON test file. is {count} but should be 2960'

    cursor.close()
    conn.close()

def test_insert_priceTimeSlots(tdb_sampledata):
    with open('tests/data/pricing_test.json', 'r', encoding='utf-8') as f:
        pricing = json.load(f)

        # inserting data twice to see check if PriceGroups are functioning properly
        for locationId in pricing.keys():
            plug_data = pricing[locationId]
            tdb_sampledata.insert_rows_in_priceTimeSlots_table(plug_data=plug_data)
        
        for locationId in pricing.keys():
            plug_data = pricing[locationId]
            tdb_sampledata.insert_rows_in_priceTimeSlots_table(plug_data=plug_data)

        conn = sqlite3.connect(f'{tdb_sampledata.name}.db')
        cursor = conn.cursor()    

        cursor.execute(f"SELECT COUNT(*) FROM priceGroups")
        count = cursor.fetchone()[0]
        assert count == 2, f'row Count in priceGroups should be 2, but is {count}'

        cursor.execute(f"SELECT SUM(mixedSpeeds) FROM priceGroups")
        sum_mixedSpeeds = cursor.fetchone()[0]
        assert sum_mixedSpeeds == 0, f'SUM(mixedSpeeds) should be zero, but is {sum_mixedSpeeds}'

        cursor.execute(f"SELECT SUM(mixedPlugTypes) FROM priceGroups")
        sum_mixedPlugTypes = cursor.fetchone()[0]
        assert sum_mixedPlugTypes == 0, f'SUM(mixedPlugTypes) should be zero, but is {sum_mixedPlugTypes}'

        cursor.execute(f"SELECT COUNT(*) FROM priceTimeSlots")
        count = cursor.fetchone()[0]
        assert count == 52, f'row Count in priceTimeSlots should be 52, but is {count}'

        cursor.close()
        conn.close()

def test_select_locationId_by_speed(tdb_sampledata):
    # ['Standard', 'Fast', 'Rapid', 'Unknown']
    slocids=tdb_sampledata.select_locationIds_by_speed('Standard')
    flocids=tdb_sampledata.select_locationIds_by_speed('Fast')
    rlocids=tdb_sampledata.select_locationIds_by_speed('Rapid')
    ulocids=tdb_sampledata.select_locationIds_by_speed('Unknown')

    scount=len(slocids)
    fcount=len(flocids)
    rcount=len(rlocids)
    ucount=len(ulocids)

    tcount=scount + fcount + rcount + ucount 

    conn = sqlite3.connect(f'{tdb_sampledata.name}.db')
    cursor = conn.cursor()    
    cursor.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT locationId, speed FROM latest_connector_groups);")
    ncount = cursor.fetchone()[0]
    assert tcount  ==  ncount, f'number of unique (location, connectorGroup) pairs should be 3145, but found {tcount}'

    cursor.close()
    conn.close()

