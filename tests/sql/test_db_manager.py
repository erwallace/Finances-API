import pytest
import os
from sqlalchemy import inspect, Table, select
from datetime import datetime
import pandas as pd
from pandas.testing import assert_frame_equal

from api import SchemaMonzo
from sql.db_manager import SQL, get_class_from_table_name, SpendingTbl, MonthsTbl

# GLOBAL FIXTURES

@pytest.fixture
def database():
    # setup
    os.environ["DEBUG"] = 'True'
    db = SQL()
    yield db
    # teardown
    with db.Session() as session:
        session.rollback()
        session.close()

@pytest.fixture
def create_all(database):
    # setup
    database.create_all_tables()
    yield
    # teardown
    database.delete_all_tables()


class TestCreateAndDelete:

    def test_get_class_from_table_name(self, database: pytest.fixture):
        class_ = get_class_from_table_name('spending')
        assert class_.__tablename__ == SpendingTbl.__tablename__

    def test_create_table_exists(self, database: pytest.fixture):
        assert 'spending' not in inspect(database.engine).get_table_names()
        database.create_table('spending')
        assert 'spending' in inspect(database.engine).get_table_names()
        # teardown
        database.delete_table('spending')

    def test_create_table_invalid_input(self, database: pytest.fixture):
        with pytest.raises(KeyError):
            database.create_table('not_a_table_name')

    def test_delete_table_exists(self, database: pytest.fixture):
        database.create_table('spending')
        assert 'spending' in inspect(database.engine).get_table_names()
        database.delete_table('spending')
        assert 'spending' not in inspect(database.engine).get_table_names()

    def test_delete_table_invalid_input(self, database: pytest.fixture):
        with pytest.raises(KeyError) as exception_info:
            database.delete_table('not_a_table_name')
        assert exception_info.match('not_a_table_name is not a valid table name.')

    def test_create_all_tables(self, database: pytest.fixture, create_all: pytest.fixture):
        tbls = ['months', 'spending', 'budget', 'accounts', 'income', 'investments_variable', 'investments_fixed']
        assert set(tbls) == set(inspect(database.engine).get_table_names())

    def test_create_all_tables_if_already_exists(self, database: pytest.fixture):
        tbls = ['months', 'spending', 'budget', 'accounts', 'income', 'investments_variable', 'investments_fixed']
        database.create_all_tables()
        database.create_all_tables()
        assert set(tbls) == set(inspect(database.engine).get_table_names())

    def test_delete_all_tables(self, database: pytest.fixture, create_all: pytest.fixture):
        database.delete_all_tables()
        assert inspect(database.engine).get_table_names() == []

    def test_delete_all_tables_if_doesnt_exist(self, database: pytest.fixture):
        database.delete_all_tables()
        assert inspect(database.engine).get_table_names() == []


class TestAppendToDB:

    @pytest.fixture
    def months_data(self):
        SCHEMA = SchemaMonzo()
        yield pd.DataFrame([{SCHEMA.ID: 'JUN 99',
                             SCHEMA.DATETIME: datetime.strptime('11/06/99', '%d/%m/%y')}])

    def test_append_to_db(self, database: pytest.fixture, create_all: pytest.fixture, months_data: pytest.fixture):
        database.append_to_db(months_data, 'months')

        table = Table(MonthsTbl.__tablename__, MonthsTbl.metadata)
        with database.engine.connect() as conn:
            from_db = pd.read_sql(sql=select(table), con=conn)

        assert_frame_equal(from_db, months_data)

    def test_append_to_db_invalid_input(self, database: pytest.fixture):
        with pytest.raises(KeyError) as exception_info:
            database.append_to_db(pd.DataFrame(), 'not_a_table_name')
        assert exception_info.match('not_a_table_name is not a valid table name.')




