from tests.functionality_testing.helper_class import tdb as db
import sqlite3 

def test_insert_connectorGroups():
    try:
        tdb=db(name='test')
        tdb.create_db()
        tdb.insert_location_test_data()
        tdb.insert_connectorGroup_test_data()

        conn = sqlite3.connect(f'{tdb.name}.db')
        cursor = conn.cursor()    
        cursor.execute(f"SELECT COUNT(*) FROM ConnectorGroups")
        count = cursor.fetchone()[0]
        assert count == 3145, f'Inserted rows do not match, entries in JSON test file. is {count} but should be 3145'

    # clean up: 
    finally:
        conn.close()
        tdb.clean_up_db()

if __name__ == '__main__':
    test_insert_connectorGroups()

