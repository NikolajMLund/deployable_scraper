from db_tools import db
import json
import os

class tdb(db): 
    """
    tdb inherits from db. But adds some functions to shorten the test scripts.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def insert_location_test_data(self):
        with open('tests/location_test.json', 'r', encoding='utf-8') as f:
            locations = json.load(f)

        for k in locations['locations'].keys():
            v = locations['locations'][k]

            self.insert_row_in_locations_table(v)


    def insert_connectorGroup_test_data(self):
        """
        
        """
        with open('tests/location_test.json', 'r', encoding='utf-8') as f:
            locations = json.load(f)


        nmissing_ConnectorCounts = 0
        for k in locations['locations'].keys():
            v = locations['locations'][k]
            try: 
                connector_dict = v['connectorCounts']
            except KeyError:
                connector_dict = v['plugTypes']
                nmissing_ConnectorCounts += 1

            for connectorGroup, connectorCount in enumerate(connector_dict):  
                self.insert_row_in_connectorGroup_table(
                    location=v, 
                    connectorGroup=connectorGroup, 
                    connectorCount=connectorCount,
                )

        print(f'for {nmissing_ConnectorCounts} locations "connectorCounts" did not exist. Used "plugTypes" instead.')

    def clean_up_db(self):
        db_file = f'{self.name}.db'

        if os.path.exists(db_file):
            os.remove(db_file)
