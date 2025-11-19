from helper_class import tdb as db
import sqlite3 

def test_foreign_keys_on_connectorGroup():
    try:
        tdb=db(name='test')

        tdb.create_db()

        tdb.insert_location_test_data()

        conn = sqlite3.connect(f'{tdb.name}.db')
        cursor = conn.cursor()    
        cursor.execute(f"SELECT COUNT(*) FROM locations")
        count = cursor.fetchone()[0]
        assert count == 2960, f'Inserted rows do not match, entries in JSON test file. is {count} but should be 2960'

        cursor.close()
        conn.close()

    # clean up: 
    finally:
        tdb.clean_up_db()

if __name__ == '__main__':
    test_foreign_keys_on_connectorGroup()
