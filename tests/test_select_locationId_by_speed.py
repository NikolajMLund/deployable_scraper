
from tests.functionality_testing.helper_class import tdb as db
import sqlite3

def test_select_locationId_by_speed():
    try:
        tdb=db(name='test')

        tdb.create_db()

        tdb.insert_location_test_data()
        tdb.insert_connectorGroup_test_data()

        # ['Standard', 'Fast', 'Rapid', 'Unknown']
        slocids=tdb.select_locationIds_by_speed('Standard')
        flocids=tdb.select_locationIds_by_speed('Fast')
        rlocids=tdb.select_locationIds_by_speed('Rapid')
        ulocids=tdb.select_locationIds_by_speed('Unknown')

        scount=len(slocids)
        fcount=len(flocids)
        rcount=len(rlocids)
        ucount=len(ulocids)

        tcount=scount + fcount + rcount + ucount 

        conn = sqlite3.connect(f'{tdb.name}.db')
        cursor = conn.cursor()    
        cursor.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT locationId, speed FROM latest_connector_groups);")
        ncount = cursor.fetchone()[0]
        assert tcount  ==  ncount, f'number of unique (location, connectorGroup) pairs should be 3145, but found {tcount}'

        cursor.close()
        conn.close()

    # clean up: 
    finally:
        tdb.clean_up_db()

if __name__ == '__main__':
    test_select_locationId_by_speed()
