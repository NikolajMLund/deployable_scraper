from helper_class import tdb as db

def test_foreign_keys_on_availabilityAggregated():
    # Setup test db. 
    try:
        tdb=db(name='test')
        tdb.create_db()

        # Populating test db. 
        ## Inserting locations into db.
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


        ## insert create_availabilityAggregated_table with right parameters
        availability_agg_insert = {
            'locationId': 'ABC',
            'revision': 1, 
            'connectorGroup': 1, 
            'availableCount': 1,
            'totalCount': 2,
            'createdAt': 100
        }
        success, error=tdb.insert_row('availabilityAggregated', availability_agg_insert)

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


        success, error=tdb.insert_row('availabilityAggregated', wrong_availability_agg_insert)

        assert error.sqlite_errorcode == 787, 'sqlite does not return 787 error when inserting non existing evseId'
    
    finally:
        tdb.clean_up_db()

if __name__ == '__main__':
    test_foreign_keys_on_availabilityAggregated()