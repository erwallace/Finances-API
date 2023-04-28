from dataclasses import dataclass
from datetime import datetime
import io
import json
from typing import List, Tuple, Any

import pandas as pd
import numpy as np

import logging
import log

import random
import string


# TODO: how am I going to add in rent?

@dataclass
class SchemaMonzo:
    ID: str = 'id'
    DATE: str = 'date'
    TIME: str = 'time'
    TYPE: str = 'type'
    NAME: str = 'name'
    EMOJI: str = 'emoji'
    SUBCATEGORY: str = 'subcategory'
    AMOUNT: str = 'amount'
    CURRENCY: str = 'currency'
    LOCAL_AMOUNT: str = 'local_amount'
    LOCAL_CURRENCY: str = 'local_currency'
    NOTES: str = 'notes'
    ADDRESS: str = 'address'
    RECEIPT: str = 'receipt'
    DESCRIPTION: str = 'description'
    SUBCATEGORY_SPLIT: str = 'subcategory_split'
    OUT: str = 'out'
    IN: str = 'in'

    # CALCULATED/CREATED
    DATETIME: str = 'date'
    MONTH_ID: str = 'month_id'
    CATEGORY: str = 'category'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.ID, self.DATE, self.TIME, self.TYPE, self.NAME, self.EMOJI, self.SUBCATEGORY, self.AMOUNT,
                self.CURRENCY, self.LOCAL_AMOUNT, self.LOCAL_CURRENCY, self.NOTES, self.ADDRESS, self.RECEIPT,
                self.DESCRIPTION, self.SUBCATEGORY_SPLIT, self.OUT, self.IN]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.ID, self.MONTH_ID, self.DATETIME, self.TYPE, self.NAME, self.CATEGORY, self.SUBCATEGORY,
                self.ADDRESS, self.DESCRIPTION, self.OUT, self.IN]


@dataclass
class SchemaAccounts:
    ACCOUNT: str = 'account'
    BALANCE: str = 'balance'
    COMMENT: str = 'comment'

    # CALCULATED/CREATED
    DATETIME: str = 'date'
    MONTH_ID: str = 'month_id'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.ACCOUNT, self.BALANCE, self.COMMENT]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.ACCOUNT, self.DATETIME, self.MONTH_ID, self.BALANCE]


@dataclass
class SchemaIncome:
    TYPE: str = 'type'
    AMOUNT: str = 'amount'
    COMMENT: str = 'comment'

    # CALCULATED/CREATED
    DATETIME: str = 'date'
    MONTH_ID: str = 'month_id'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.TYPE, self.AMOUNT, self.COMMENT]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.TYPE, self.DATETIME, self.MONTH_ID, self.AMOUNT]


@dataclass
class SchemaInvestmentVariable:
    NAME: str = 'name'
    COMPANY: str = 'company'
    UNIT_PRICE: str = 'unit_price'
    UNITS_OWNED: str = 'units_owned'
    DATETIME: str = 'date'

    # CALCULATED/CREATED
    MONTH_ID: str = 'month_id'
    VALUE: str = 'value'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.NAME, self.COMPANY, self.UNIT_PRICE, self.UNITS_OWNED, self.DATETIME]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.NAME, self.DATETIME, self.MONTH_ID, self.COMPANY, self.UNIT_PRICE, self.UNITS_OWNED, self.VALUE]


@dataclass
class SchemaInvestmentFixed:
    NAME: str = 'name'
    COMPANY: str = 'company'
    AMOUNT: str = 'amount'
    INTEREST: str = 'interest_%'
    DURATION: str = 'duration_months'
    MATURITY_DATE: str = 'maturity_date'

    # CALCULATED/CREATED
    RETURN: str = 'return'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.NAME, self.COMPANY, self.AMOUNT, self.INTEREST, self.DURATION, self.MATURITY_DATE]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.NAME, self.COMPANY, self.AMOUNT, self.INTEREST, self.DURATION, self.MATURITY_DATE, self.RETURN]


class Budget:
    MONTH_FORMAT: str = '%b %y'

    def add_month_id_column(self, df: pd.DataFrame) -> pd.Series:
        '''adds month column (MMM YY) to existing dataframe based on datetime column'''

        extract_MMM_YY = lambda x: datetime.strftime(x, self.MONTH_FORMAT).upper()

        return df[self.SCHEMA.DATETIME].apply(extract_MMM_YY)


class Monzo(Budget):
    DATETIME_FORMAT: str = '%d/%m/%Y %H:%M:%S'
    SKIPROWS: int = 1
    SCHEMA: SchemaMonzo = SchemaMonzo()

    def preprocess(self, log_file: str | io.BytesIO | io.StringIO) -> tuple[pd.DataFrame, pd.DataFrame]:
        '''loads Monzo file and returns as pandas dataframe'''
        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        # add additional information
        df[self.SCHEMA.DATETIME] = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df[self.SCHEMA.CATEGORY] = self.add_category_column(df)

        df = self.split_subcategory_payments(df)

        df = df[self.SCHEMA.df_columns_final]

        months = df[self.SCHEMA.MONTH_ID].unique()
        dt = [datetime.strptime(m, self.MONTH_FORMAT) for m in months]
        months = pd.DataFrame({self.SCHEMA.MONTH_ID: months,
                               self.SCHEMA.DATETIME: dt})

        return df, months

    def add_datetime_column(self, df: pd.DataFrame) -> pd.Series:
        '''adds datetime column to existing dataframe based on date and time columns'''

        df['tmp'] = df[self.SCHEMA.DATE] + ' ' + df[self.SCHEMA.TIME]
        extract_datetime = lambda x: datetime.strptime(x, self.DATETIME_FORMAT)

        return df['tmp'].apply(extract_datetime)

    def add_category_column(self, df: pd.DataFrame) -> pd.Series:
        '''adds category column to existing dataframe based on subcategory column and json mapping'''

        # read in configurations from json
        with open("sub_category.json") as jsn:
            mapper = jsn.read()

        # decoding the JSON to dictionary
        sub_category = json.loads(mapper)

        df_subcategories = set(df[self.SCHEMA.SUBCATEGORY])
        json_subcategories = set(sub_category.keys())
        diff = df_subcategories.difference(json_subcategories)
        assert diff != {}, f'Monzo statement contains subcategories missing from sub_category.json ({diff})'

        return df[self.SCHEMA.SUBCATEGORY].map(sub_category)

    def split_subcategory_payments(self, df: pd.DataFrame) -> pd.DataFrame:
        '''any "subcategory split" payments will be separated into individual rows'''
        df_keep = df[df[self.SCHEMA.SUBCATEGORY_SPLIT].isna()]
        df_split = df[~df[self.SCHEMA.SUBCATEGORY_SPLIT].isna()]

        for _, row in df_split.iterrows():

            split = row[self.SCHEMA.SUBCATEGORY_SPLIT].split(',')

            for subcategory_value in split:

                subcategory, value = tuple(subcategory_value.split(':'))
                new_row = row
                new_row[self.SCHEMA.ID] = 'tx_0000AT' + ''.join(random.sample(string.ascii_letters + string.digits, 16))
                new_row[self.SCHEMA.SUBCATEGORY] = subcategory
                new_row[self.SCHEMA.SUBCATEGORY_SPLIT] = np.nan

                if float(value) > 0:
                    new_row[self.SCHEMA.IN] = value
                else:
                    new_row[self.SCHEMA.OUT] = value

                df_keep = pd.concat([df_keep, pd.DataFrame([new_row])], ignore_index=True)

        return df_keep.reset_index(drop=True)


class Accounts(Budget):
    DATETIME_FORMAT: str = '%d/%m/%Y'
    SKIPROWS: int = 1
    SCHEMA: SchemaAccounts = SchemaAccounts()

    def preprocess(self, log_file='inputs/accounts.csv') -> pd.DataFrame:
        '''loads accounts file and returns as pandas dataframe'''

        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df = df[self.SCHEMA.df_columns_final]

        return df

    def add_datetime_column(self, df: pd.DataFrame) -> pd.DataFrame:
        '''adds datetime column to existing dataframe based on date row, then remove that row'''

        dt_idx = df.index[df[self.SCHEMA.ACCOUNT] == 'date']
        date = df.iloc[dt_idx][self.SCHEMA.BALANCE].values[0]
        df[self.SCHEMA.DATETIME] = datetime.strptime(date, self.DATETIME_FORMAT)
        df = df.drop(dt_idx, axis=0)

        return df


class Income(Budget):
    DATETIME_FORMAT: str = '%d/%m/%Y'
    SKIPROWS: int = 1
    SCHEMA: SchemaIncome = SchemaIncome()

    def preprocess(self, log_file='inputs/income.csv') -> pd.DataFrame:
        '''loads income file and returns as pandas dataframe'''

        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df = df[self.SCHEMA.df_columns_final]

        return df

    def add_datetime_column(self, df: pd.DataFrame) -> pd.DataFrame:
        '''adds datetime column to existing dataframe based on date row, then remove that row'''

        dt_idx = df.index[df[self.SCHEMA.TYPE] == 'date']
        date = df.iloc[dt_idx][self.SCHEMA.AMOUNT].values[0]
        df[self.SCHEMA.DATETIME] = datetime.strptime(date, self.DATETIME_FORMAT)
        df = df.drop(dt_idx, axis=0)

        return df


class InvestmentVariable(Budget):
    DATETIME_FORMAT: str = '%d/%m/%Y'
    SKIPROWS: int = 1
    SCHEMA: SchemaInvestmentVariable = SchemaInvestmentVariable()

    def preprocess(self, log_file='inputs/investments_variable.csv') -> pd.DataFrame:
        '''loads investments_variable file and returns as pandas dataframe'''
        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df[self.SCHEMA.DATETIME] = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df[self.SCHEMA.VALUE] = self.add_value_column(df)
        df = df[self.SCHEMA.df_columns_final]

        return df

    def add_datetime_column(self, df: pd.DataFrame) -> pd.Series:
        '''adds datetime column to existing dataframe based on date and time columns'''

        extract_datetime = lambda x: datetime.strptime(x, self.DATETIME_FORMAT)

        return df[self.SCHEMA.DATETIME].apply(extract_datetime)

    def add_value_column(self, df: pd.DataFrame) -> pd.Series:
        '''adds value column to existing dataframe based on unit_price and units_owned columns'''
        val = df[self.SCHEMA.UNIT_PRICE] * df[self.SCHEMA.UNITS_OWNED]
        return val.round(2)


class InvestmentFixed(Budget):
    SKIPROWS: int = 1
    SCHEMA: SchemaInvestmentFixed = SchemaInvestmentFixed()

    def preprocess(self, log_file='inputs/investments_fixed.csv') -> pd.DataFrame:
        '''loads investments_fixed file and returns as pandas dataframe'''
        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df[self.SCHEMA.RETURN] = self.add_return_column(df)
        df = df[self.SCHEMA.df_columns_final]

        return df

    def add_return_column(self, df: pd.DataFrame) -> pd.Series:

        df['duration_yrs'] = df[self.SCHEMA.DURATION]//12 + (df[self.SCHEMA.DURATION]%12)/12
        rtn = df['duration_yrs'] * df[self.SCHEMA.AMOUNT] * (df[self.SCHEMA.INTEREST]/100)
        return rtn.round(2)

# example
mz = Monzo()
df_mz, months = mz.preprocess('statements/MonzoDataExport_March_2023-04-01_085048.csv')

acc = Accounts()
df_acc = acc.preprocess()

inc = Income()
df_inc = inc.preprocess()

inv = InvestmentVariable()
df_inv = inv.preprocess()

inf = InvestmentFixed()
df_inf = inf.preprocess()

print('done')

