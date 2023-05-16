from sqlalchemy import text, create_engine
import pandas as pd
import logging
import log
import psycopg2


class SQL:

    def __init__(self, address='postgresql://postgres:zeropetroleum@localhost:5432/pybudget'):

        '''
        address = postgresql://username:password@hostname:5432/db_name

        username: postgres
        password: zeropetroleum
        hostname: localhost
        db_name: pybudget
        '''
        self.engine = create_engine(address)
        logging.info(f'SQL connection established to {address}')

    def create_months(self):
        """creates the SQL months table in the database"""

        with self.engine.begin() as conn:
            conn.execute(
                text('CREATE TABLE months ('
                     '"month_id" CHAR(6) PRIMARY KEY,'
                     '"Date" TIMESTAMP)'
                     )
            )
        logging.info(f'table created: months')

    def create_spending_data(self):
        """creates the SQL spending_data table in the database"""

        with self.engine.begin() as conn:
            conn.execute(
                text('CREATE TABLE spending_data ('
                     '"id" CHAR(11) PRIMARY KEY,'
                     '"month_id" CHAR(6) REFERENCES months (month_id),'
                     '"Date" TIMESTAMP,'
                     '"Type" VARCHAR,'
                     '"Name" VARCHAR,'
                     '"Category" VARCHAR,'
                     '"Subcategory" VARCHAR,'
                     '"Address" VARCHAR,'
                     '"Description" VARCHAR,'
                     '"Out" INTEGER,'
                     '"In" INTEGER)'
                     )
            )
        logging.info(f'table created: spending_data')

    def create_budget(self):
        """creates the SQL budget table in the database"""

        with self.engine.begin() as conn:
            conn.execute(
                text('CREATE TABLE budget ('
                     '"id" CHAR(11) PRIMARY KEY,'
                     '"month_id" CHAR(6) REFERENCES months (month_id),'
                     '"Date" TIMESTAMP,'
                     '"Category" VARCHAR,'
                     '"Subcategory" VARCHAR,'
                     '"Budget" INTEGER)'
                     )
            )
        logging.info(f'table created: budget')

    def create_accounts(self):
        """creates the SQL accounts table in the database"""

        with self.engine.begin() as conn:
            conn.execute(
                text('CREATE TABLE accounts ('
                     '"id" CHAR(11) PRIMARY KEY,'
                     '"Account" VARCHAR,'
                     '"Date" TIMESTAMP,'
                     '"month_id" CHAR(6) REFERENCES months (month_id),'
                     '"Balance" INTEGER)'
                     )
            )
        logging.info(f'table created: accounts')

    def create_income(self):
        """creates the SQL income table in the database"""

        with self.engine.begin() as conn:
            conn.execute(
                text('CREATE TABLE income ('
                     '"id" CHAR(11) PRIMARY KEY,'
                     '"Type" VARCHAR,'
                     '"Date" TIMESTAMP,'
                     '"month_id" CHAR(6) REFERENCES months (month_id),'
                     '"Amount" INTEGER)'
                     )
            )
        logging.info(f'table created: income')

    def create_investments_variable(self):
        """creates the SQL investments_variable table in the database"""

        with self.engine.begin() as conn:
            conn.execute(
                text('CREATE TABLE investments_variable ('
                     '"id" CHAR(11) PRIMARY KEY,'
                     '"Name" VARCHAR,'
                     '"Date" TIMESTAMP,'
                     '"month_id" CHAR(6) REFERENCES months (month_id),'
                     '"Company" VARCHAR,'
                     '"Unit Price" NUMERIC,'
                     '"Units Owned" NUMERIC,'
                     '"Value" DECIMAL(12,2))'
                     )
            )
        logging.info(f'table created: investments_variable')

    def create_investments_fixed(self):
        """creates the SQL investments_fixed table in the database"""

        with self.engine.begin() as conn:
            conn.execute(
                text('CREATE TABLE investments_fixed ('
                     '"id" VARCHAR PRIMARY KEY,'
                     '"Name" VARCHAR,'
                     '"Company" VARCHAR,'
                     '"Amount" INTEGER,'
                     '"Interest (%)" NUMERIC,'
                     '"Months" INTEGER,'
                     '"Purchased" TIMESTAMP,'
                     '"Matures" TIMESTAMP,'
                     '"Return" INTEGER)'
                     )
            )
        logging.info(f'table created: investments_fixed')

    def create_all_tables(self):
        self.create_months()
        self.create_spending_data()
        self.create_budget()
        self.create_accounts()
        self.create_income()
        self.create_investments_variable()
        self.create_investments_fixed()

    def delete_table(self, table_name):
        """deletes a table from the pybudget"""

        with self.engine.begin() as conn:
            conn.execute(text('DROP TABLE ' + table_name))
        logging.info(f'table deleted: {table_name}')

    def delete_all_tables(self):
        tbls = ['spending_data', 'budget', 'accounts', 'income', 'investments_variable', 'investments_fixed', 'months']

        for tbl in tbls:
            try:
                self.delete_table(tbl)
            except:
                logging.warning(f'table not found, cannot be deleted: {tbl}')

    def append_to_db(self, df: pd.DataFrame, table_name: str):
        """check for duplicates and append to spending_data table in database"""

        # TODO: use schema values (?)
        primary_keys = {'months': 'month_id',
                        'spending_data': 'id',
                        'budget': 'id',
                        'accounts': 'id',
                        'income': 'id',
                        'investments_variable': 'id',
                        'investments_fixed': 'id'
                        }
        assert table_name in primary_keys.keys(), f'{table_name} is not a valid table name.'
        p_key = primary_keys[table_name]

        with self.engine.connect() as conn:

            db = pd.read_sql(sql=text(f'SELECT {p_key} FROM {table_name}'),
                             con=conn
                             )

        # append non-duplicate rows
        non_duplicates = df[df[p_key].isin(db[p_key]) == False].dropna(how='all')
        # error if no rows are would be appended
        if non_duplicates.shape[0] == 0:
            logging.error(f'NO ROWS FROM DF APPENDED TO {table_name.upper()} (sqlalchemy.exc.IntegrityError)')
            return

        non_duplicates.to_sql(name=table_name,
                              con=self.engine,
                              if_exists='append',
                              index=False
                              )

        logging.info(f'({non_duplicates.shape[0]}/{df.shape[0]}) rows from df appended to {table_name}')
