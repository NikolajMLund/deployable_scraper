from helper_class import tdb as db

def test_create_db():
    tdb = db(name='test')

    if tdb.check_if_db_exists():
        #print(f'To run this test first delete the {tdb.name}.db in the root directory')
        raise FileExistsError(f'To run this test first delete the {tdb.name}.db in the root directory')

    tdb.create_db()
    try:
        assert tdb.check_if_db_exists(), f'{tdb.name}.db was not created'
    finally:
        tdb.clean_up_db()

if __name__ == '__main__':
    test_create_db()