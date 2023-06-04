from api import SchemaMonzo
import pytest
import os
from db_manager import SQL

class TestQueries:

    @pytest.fixture
    def database(self):
        # setup
        os.environ["DEBUG"] = 'True'
        db = SQL()
        db.create_all_tables()
        yield db
        # teardown
        with db.Session() as session:
            session.rollback()
            session.close()

    def test_query_db(self):
        pass

    def test_query_inv_var(self):
        pass

    def test_query_inv_fix(self):
        pass


class TestCreatingTables:

    def test_spending_table(self):
        pass

    def test_summary_table(self):
        pass

    def test_accounts_table(self):
        pass

    def test_income_table(self):
        pass

    def test_investment_tables(self):
        pass
