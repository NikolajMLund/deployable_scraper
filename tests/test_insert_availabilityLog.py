from helper_class import tdb as db
import json
import sqlite3 

def test_insert_availabilityLog():
    try:
        # creating test environment.
        tdb=db(name='test')
        tdb.create_db()
        tdb.insert_location_test_data()

        with open('tests/availability_test.json', 'r', encoding='utf-8') as f:
            availability = json.load(f)

        for k in availability.keys():
            v = availability[k]
            tdb.insert_row_in_availabilityLog_table(loc_avail_query=v)

        conn = sqlite3.connect(f'{tdb.name}.db')
        cursor = conn.cursor()    
        cursor.execute(f"SELECT COUNT(*) FROM availabilityLog")
        count_availabilityLog = cursor.fetchone()[0]
        assert count_availabilityLog == 30, f'there should be 30 entries, but db have {count_availabilityLog}'

        cursor.execute(f"SELECT COUNT(*) FROM evseIds")
        count_evseIds = cursor.fetchone()[0]
        assert count_evseIds == 30, f'there should be 30 entries, but db have {count_evseIds}'

    finally:
        conn.close()
        # clean up: 
        tdb.clean_up_db()

if __name__ == '__main__':
    test_insert_availabilityLog()
