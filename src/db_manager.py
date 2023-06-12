from sqlalchemy import create_engine, Column, String, DateTime, Integer, Float, ForeignKey, select, inspect, Table, \
    TIMESTAMP
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import logging
import psycopg2
import os
from api import SchemaMonzo, SchemaInputs, SchemaInvestmentFixed, SchemaInvestmentVariable

import logging
from log import get_logger
logger = get_logger(__name__)

Base = declarative_base()


class MonthsTbl(Base):
    ''' sqlalchemy table class for months '''
    __tablename__ = 'months'
    SCHEMA = SchemaMonzo()

    id = Column(String, primary_key=True)
    locals()[SCHEMA.DATETIME] = Column(DateTime)

    def __repr__(self):
        return f'<SpendingTbl({self.SCHEMA.MONTH_ID}={getattr(self, self.SCHEMA.MONTH_ID)},' \
               f'{self.SCHEMA.DATETIME}={getattr(self, self.SCHEMA.DATETIME)})>'


class SpendingTbl(Base):
    ''' sqlalchemy table class for spending '''
    __tablename__ = 'spending'
    SCHEMA = SchemaMonzo()

    id = Column(String, primary_key=True)
    locals()[SCHEMA.MONTH_ID] = Column(String, ForeignKey('months.id'))
    locals()[SCHEMA.DATETIME] = Column(DateTime)
    locals()[SCHEMA.TYPE] = Column(String)
    locals()[SCHEMA.NAME] = Column(String)
    locals()[SCHEMA.CATEGORY] = Column(String)  # category
    locals()[SCHEMA.SUBCATEGORY] = Column(String)  # category
    locals()[SCHEMA.ADDRESS] = Column(String)
    locals()[SCHEMA.DESCRIPTION] = Column(String)
    locals()[SCHEMA.OUT] = Column(Integer)
    locals()[SCHEMA.IN] = Column(Integer)

    def __repr__(self):
        return f'<SpendingTbl({self.SCHEMA.ID}={getattr(self, self.SCHEMA.ID)},' \
               f'{self.SCHEMA.MONTH_ID}={getattr(self, self.SCHEMA.MONTH_ID)},' \
               f'{self.SCHEMA.DATETIME}={getattr(self, self.SCHEMA.DATETIME)},' \
               f'{self.SCHEMA.TYPE}={getattr(self, self.SCHEMA.TYPE)},' \
               f'{self.SCHEMA.NAME}={getattr(self, self.SCHEMA.NAME)},' \
               f'{self.SCHEMA.CATEGORY}={getattr(self, self.SCHEMA.CATEGORY)},' \
               f'{self.SCHEMA.SUBCATEGORY}={getattr(self, self.SCHEMA.SUBCATEGORY)},' \
               f'{self.SCHEMA.ADDRESS}={getattr(self, self.SCHEMA.ADDRESS)},' \
               f'{self.SCHEMA.DESCRIPTION}={getattr(self, self.SCHEMA.DESCRIPTION)},' \
               f'{self.SCHEMA.OUT}={getattr(self, self.SCHEMA.OUT)},' \
               f'{self.SCHEMA.IN}={getattr(self, self.SCHEMA.IN)})>'


class BudgetTbl(Base):
    ''' sqlalchemy table class for budget '''
    __tablename__ = 'budget'
    SCHEMA = SchemaInputs()

    id = Column(String, primary_key=True)
    locals()[SCHEMA.MONTH_ID] = Column(String, ForeignKey('months.id'))
    locals()[SCHEMA.DATETIME] = Column(DateTime)
    locals()[SCHEMA.CATEGORY] = Column(String)
    locals()[SCHEMA.SUBCATEGORY] = Column(String)
    locals()[SCHEMA.BUDGET] = Column(Integer)

    def __repr__(self):
        return f'<BudgetTbl({self.SCHEMA.ID}={getattr(self, self.SCHEMA.ID)},' \
               f'{self.SCHEMA.MONTH_ID}={getattr(self, self.SCHEMA.MONTH_ID)},' \
               f'{self.SCHEMA.DATETIME}={getattr(self, self.SCHEMA.DATETIME)},' \
               f'{self.SCHEMA.CATEGORY}={getattr(self, self.SCHEMA.CATEGORY)},' \
               f'{self.SCHEMA.SUBCATEGORY}={getattr(self, self.SCHEMA.SUBCATEGORY)},' \
               f'{self.SCHEMA.BUDGET}={getattr(self, self.SCHEMA.BUDGET)})>'


class AccountsTbl(Base):
    ''' sqlalchemy table class for accounts '''
    __tablename__ = 'accounts'
    SCHEMA = SchemaInputs()

    id = Column(String, primary_key=True)
    locals()[SCHEMA.ACCOUNT] = Column(String)
    locals()[SCHEMA.DATETIME] = Column(DateTime)
    locals()[SCHEMA.MONTH_ID] = Column(String, ForeignKey('months.id'))
    locals()[SCHEMA.BALANCE] = Column(Integer)

    def __repr__(self):
        return f'<AccountsTbl({self.SCHEMA.ID}={getattr(self, self.SCHEMA.ID)},' \
               f'{self.SCHEMA.ACCOUNT}={getattr(self, self.SCHEMA.ACCOUNT)},' \
               f'{self.SCHEMA.DATETIME}={getattr(self, self.SCHEMA.DATETIME)},' \
               f'{self.SCHEMA.MONTH_ID}={getattr(self, self.SCHEMA.MONTH_ID)},' \
               f'{self.SCHEMA.BALANCE}={getattr(self, self.SCHEMA.BALANCE)})>'


class IncomeTbl(Base):
    ''' sqlalchemy table class for income '''
    __tablename__ = 'income'
    SCHEMA = SchemaInputs

    id = Column(String, primary_key=True)
    locals()[SCHEMA.TYPE] = Column(String)
    locals()[SCHEMA.DATETIME] = Column(DateTime)
    locals()[SCHEMA.MONTH_ID] = Column(String, ForeignKey('months.id'))
    locals()[SCHEMA.AMOUNT] = Column(Integer)

    def __repr__(self):
        return f'<IncomeTbl({self.SCHEMA.ID}={getattr(self, self.SCHEMA.ID)},' \
               f'{self.SCHEMA.TYPE}={getattr(self, self.SCHEMA.TYPE)},' \
               f'{self.SCHEMA.DATETIME}={getattr(self, self.SCHEMA.DATETIME)},' \
               f'{self.SCHEMA.MONTH_ID}={getattr(self, self.SCHEMA.MONTH_ID)},' \
               f'{self.SCHEMA.AMOUNT}={getattr(self, self.SCHEMA.AMOUNT)})>'


class InvestmentsVariableTbl(Base):
    ''' sqlalchemy table class for investments_variable '''
    __tablename__ = 'investments_variable'
    SCHEMA = SchemaInvestmentVariable()

    id = Column(String, primary_key=True)
    locals()[SCHEMA.NAME] = Column(String)
    locals()[SCHEMA.DATETIME] = Column(DateTime)
    locals()[SCHEMA.MONTH_ID] = Column(String, ForeignKey('months.id'))
    locals()[SCHEMA.COMPANY] = Column(String)
    locals()[SCHEMA.UNIT_PRICE] = Column(Float)
    locals()[SCHEMA.UNITS_OWNED] = Column(Float)
    locals()[SCHEMA.VALUE] = Column(Float)

    def __repr__(self):
        return f'<InvestmentsVariableTbl({self.SCHEMA.ID}={getattr(self, self.SCHEMA.ID)},' \
               f'{self.SCHEMA.NAME}={getattr(self, self.SCHEMA.NAME)},' \
               f'{self.SCHEMA.DATETIME}={getattr(self, self.SCHEMA.DATETIME)},' \
               f'{self.SCHEMA.MONTH_ID}={getattr(self, self.SCHEMA.MONTH_ID)},' \
               f'{self.SCHEMA.COMPANY}={getattr(self, self.SCHEMA.COMPANY)},' \
               f'{self.SCHEMA.UNIT_PRICE}={getattr(self, self.SCHEMA.UNIT_PRICE)},' \
               f'{self.SCHEMA.UNITS_OWNED}={getattr(self, self.SCHEMA.UNITS_OWNED)},' \
               f'{self.SCHEMA.VALUE}={getattr(self, self.SCHEMA.VALUE)})>'


class InvestmentsFixedTbl(Base):
    ''' sqlalchemy table class for investments_fixed '''
    __tablename__ = 'investments_fixed'
    SCHEMA = SchemaInvestmentFixed()

    id = Column(String, primary_key=True)
    locals()[SCHEMA.NAME] = Column(String)
    locals()[SCHEMA.COMPANY] = Column(String)
    locals()[SCHEMA.AMOUNT] = Column(Integer)
    locals()[SCHEMA.INTEREST] = Column(Float)
    locals()[SCHEMA.DURATION] = Column(Integer)
    locals()[SCHEMA.PURCHASE_DATE] = Column(TIMESTAMP)
    locals()[SCHEMA.MATURITY_DATE] = Column(TIMESTAMP)
    locals()[SCHEMA.RETURN] = Column(Integer)

    def __repr__(self):
        return f'<InvestmentsFixed({self.SCHEMA.ID}={getattr(self, self.SCHEMA.ID)},' \
               f'{self.SCHEMA.NAME}={getattr(self, self.SCHEMA.NAME)},' \
               f'{self.SCHEMA.COMPANY}={getattr(self, self.SCHEMA.COMPANY)},' \
               f'{self.SCHEMA.AMOUNT}={getattr(self, self.SCHEMA.AMOUNT)},' \
               f'{self.SCHEMA.INTEREST}={getattr(self, self.SCHEMA.INTEREST)},' \
               f'{self.SCHEMA.DURATION}={getattr(self, self.SCHEMA.DURATION)},' \
               f'{self.SCHEMA.PURCHASE_DATE}={getattr(self, self.SCHEMA.PURCHASE_DATE)},' \
               f'{self.SCHEMA.MATURITY_DATE}={getattr(self, self.SCHEMA.MATURITY_DATE)},' \
               f'{self.SCHEMA.RETURN}={getattr(self, self.SCHEMA.RETURN)})>'


class SQL:

    def __init__(self):

        if os.getenv("DEBUG") == 'True':
            address = r'sqlite:///data/debug.db'
        else:
            address = r'sqlite:///data/spending.db'

        self.engine = create_engine(address)
        self.Session = sessionmaker(bind=self.engine)
        logging.info(f'SQL connection established to {address}')

    def create_table(self, table_name: str) -> None:
        '''
        creates a table for the database

        :param table_name: str __tablename__ of a sqlalchemy table class

        :return: None
        '''
        class_ = get_class_from_table_name(table_name)
        class_.__table__.create(bind=self.engine)
        logging.info(f'table created: {table_name}')

    def create_all_tables(self) -> None:
        ''' creates all tables in the schema '''
        tbls = ['months', 'spending', 'budget', 'accounts', 'income', 'investments_variable', 'investments_fixed']
        for tbl in tbls:
            self.create_table(tbl)
        '''
        for tbl in tbls:
            try:
                self.create_table(tbl)
            except:
                logging.warning(f'table already exists: {tbl}')
        '''

    def delete_table(self, table_name: str) -> None:
        '''
        deletes a table from the database

        :param table_name: str __tablename__ of a sqlalchemy table class

        :return: None
        '''
        if table_name not in inspect(self.engine).get_table_names():
            raise KeyError(f'{table_name} is not a valid table name.')

        class_ = get_class_from_table_name(table_name)
        class_.__table__.drop(self.engine)
        logging.info(f'table deleted: {table_name}')

    def delete_all_tables(self) -> None:
        ''' deletes all tables from the schema '''
        tbls = ['spending', 'budget', 'accounts', 'income', 'investments_variable', 'investments_fixed', 'months']

        for tbl in tbls:
            try:
                self.delete_table(tbl)
            except:
                logging.warning(f'table not found, cannot be deleted: {tbl}')

    def append_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        '''
        Appends df to specified table_name. Only rows not already present in the database will be appended.

        :param df: pd.DataFrame of values to be added to table
        :param table_name: str __tablename__ of a sqlalchemy table class

        :return: None
        '''

        if table_name not in inspect(self.engine).get_table_names():
            raise KeyError(f'{table_name} is not a valid table name.')

        class_ = get_class_from_table_name(table_name)
        table = Table(class_.__tablename__, class_.metadata)
        p_key = inspect(class_).primary_key[0].name
        with self.engine.connect() as conn:
            db = pd.read_sql(sql=select(table.c.id), con=conn)  # FROM is implicit

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


def get_class_from_table_name(table_name: str) -> object:
    '''
    Given a sqlalchemy table class __tablename__, this returns the class object

    :param table_name: str __tablename__ of a sqlalchemy table class

    :return: class object
    '''
    table_name_to_class = {m.tables[0].name: m.class_ for m in Base.registry.mappers}
    return table_name_to_class[table_name]


if __name__ == '__main__':
    db = SQL()
    db.delete_all_tables()
    db.create_all_tables()

    pass
