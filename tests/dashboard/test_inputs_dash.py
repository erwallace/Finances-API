import pytest
import os
from sql.db_manager import SQL

# python -m pytest --rootdir=src/  [expect this to work]
# pytest config.py look for extra options
# pytest cant find src modules

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
