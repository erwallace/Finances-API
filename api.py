from dataclasses import dataclass
from datetime import datetime
import io
import os
import json
from typing import List

import fnmatch

import pandas as pd
import numpy as np

import logging
import log


@dataclass
class SchemaMonzo:
    ID: str = 'id'
    DATE: str = 'Date'
    TIME: str = 'Time'
    TYPE: str = 'Type'
    NAME: str = 'Name'
    EMOJI: str = 'Emoji'
    SUBCATEGORY: str = 'Subcategory'
    AMOUNT: str = 'Amount'
    CURRENCY: str = 'Currency'
    LOCAL_AMOUNT: str = 'Local Amount'
    LOCAL_CURRENCY: str = 'Local Currency'
    NOTES: str = 'Notes'
    ADDRESS: str = 'Address'
    RECEIPT: str = 'Receipt'
    DESCRIPTION: str = 'Description'
    SUBCATEGORY_SPLIT: str = 'Subcategory Split'
    OUT: str = 'Out'
    IN: str = 'In'

    # CALCULATED/CREATED
    DATETIME: str = 'Date'
    MONTH_ID: str = 'month_id'
    CATEGORY: str = 'Category'
    BALANCE: str = 'Balance'
    TOTAL: str = 'Total'
    DIFFERENCE: str = 'Diff.'

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
class SchemaBudget:
    SUBCATEGORY: str = 'Subcategory'
    BUDGET: str = 'Budget'
    COMMENT: str = 'Comment'

    # CALCULATED/CREATED
    ID: str = 'id'
    DATETIME: str = 'Date'
    MONTH_ID: str = 'month_id'
    CATEGORY: str = 'Category'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.SUBCATEGORY, self.BUDGET, self.COMMENT]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.ID, self.MONTH_ID, self.DATETIME, self.CATEGORY, self.SUBCATEGORY, self.BUDGET]


@dataclass
class SchemaAccounts:
    ACCOUNT: str = 'Account'
    BALANCE: str = 'Balance'
    COMMENT: str = 'Comment'

    # CALCULATED/CREATED
    ID: str = 'id'
    DATETIME: str = 'Date'
    MONTH_ID: str = 'month_id'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.ACCOUNT, self.BALANCE, self.COMMENT]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.ID, self.ACCOUNT, self.DATETIME, self.MONTH_ID, self.BALANCE]


@dataclass
class SchemaIncome:
    TYPE: str = 'Type'
    AMOUNT: str = 'Amount'
    COMMENT: str = 'Comment'

    # CALCULATED/CREATED
    ID: str = 'id'
    DATETIME: str = 'Date'
    MONTH_ID: str = 'month_id'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.TYPE, self.AMOUNT, self.COMMENT]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.ID, self.TYPE, self.DATETIME, self.MONTH_ID, self.AMOUNT]


@dataclass
class SchemaInvestmentVariable:
    NAME: str = 'Name'
    COMPANY: str = 'Company'
    UNIT_PRICE: str = 'Unit Price'
    UNITS_OWNED: str = 'Units Owned'
    DATETIME: str = 'Date'

    # CALCULATED/CREATED
    ID: str = 'id'
    MONTH_ID: str = 'month_id'
    VALUE: str = 'Value'
    D_UNIT_PRICE: str = 'd. Unit Price (%)'
    D_VALUE: str = 'd. Value'
    UNIT_PRICE_PREV: str = 'Unit Price_prev'
    VALUE_PREV: str = 'Value_prev'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.NAME, self.COMPANY, self.UNIT_PRICE, self.UNITS_OWNED, self.DATETIME]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.ID, self.NAME, self.DATETIME, self.MONTH_ID, self.COMPANY, self.UNIT_PRICE, self.UNITS_OWNED, self.VALUE]


@dataclass
class SchemaInvestmentFixed:
    NAME: str = 'Name'
    COMPANY: str = 'Company'
    AMOUNT: str = 'Amount'
    INTEREST: str = 'Interest (%)'
    DURATION: str = 'Months'
    PURCHASE_DATE: str = 'Purchased'
    MATURITY_DATE: str = 'Matures'

    # CALCULATED/CREATED
    ID: str = 'id'
    RETURN: str = 'Return'

    @property
    def df_columns_initial(self) -> List[str]:
        '''provides all the columns in necessary order for correct extraction to dataframe'''
        return [self.NAME, self.COMPANY, self.AMOUNT, self.INTEREST, self.DURATION, self.PURCHASE_DATE, self.MATURITY_DATE]

    @property
    def df_columns_final(self) -> List[str]:
        '''provides all the columns in necessary order for correct exporting of dataframe'''
        return [self.ID, self.NAME, self.COMPANY, self.AMOUNT, self.INTEREST, self.DURATION, self.PURCHASE_DATE, self.MATURITY_DATE, self.RETURN]


class Finances:
    MONTH_FORMAT: str = '%b %y'

    def __init__(self, month_id: str):
        ''' month_id in the format "MMM YY" '''
        self.month_id = month_id

    def add_month_id_column(self, df: pd.DataFrame) -> pd.Series:
        '''adds month column (MMM YY) to existing dataframe based on datetime column'''

        extract_MMM_YY = lambda x: datetime.strftime(x, self.MONTH_FORMAT).upper()

        return df[self.SCHEMA.DATETIME].apply(extract_MMM_YY)

    def add_id_column(self, df: pd.DataFrame) -> pd.Series:
        '''adds id column to existing dataframe based on month_id and index. To be used as primary key'''

        df = df.reset_index(drop=True).reset_index(names='idx')
        df.idx = df.idx.astype(str).str.zfill(4)

        return df[self.SCHEMA.MONTH_ID] + ' ' + df.idx

    def convert_to_pennies(self, col: pd.Series) -> pd.Series:
        '''converts all money to pennies so that it can be stored as an integer'''
        col = col.astype(float)*100

        return col.fillna(0).astype(int)


class Monzo(Finances):
    DATETIME_FORMAT: str = '%d/%m/%Y %H:%M:%S'
    SKIPROWS: int = 1
    SCHEMA: SchemaMonzo = SchemaMonzo()

    def preprocess(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        '''loads Monzo file and returns as pandas dataframe'''

        cwd = os.getcwd()
        log_folder = cwd + '\\statements'
        dt = datetime.strptime(self.month_id, self.MONTH_FORMAT)
        month = datetime.strftime(dt, '%B')
        year = datetime.strftime(dt, '%Y')
        file = fnmatch.filter(os.listdir(log_folder), f'MonzoDataExport_{month}_{year}*.csv')
        assert len(file) != 0, f'No files found in cwd matching MonzoDataExport_{month}_{year}*.csv'
        assert len(file) == 1, f'More than one file matches MonzoDataExport_{month}_{year}*.csv - {file}'
        log_file = log_folder + '\\' + file[0]

        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        # add additional information
        df[self.SCHEMA.DATETIME] = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df[self.SCHEMA.CATEGORY] = self.add_category_column(df)
        df = self.split_subcategory_payments(df)
        df[self.SCHEMA.IN] = self.convert_to_pennies(df[self.SCHEMA.IN])
        df[self.SCHEMA.OUT] = self.convert_to_pennies(df[self.SCHEMA.OUT])
        df[self.SCHEMA.ID] = self.add_id_column(df)

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
        assert diff == set(), f'Monzo statement contains subcategories missing from sub_category.json ({diff})'

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
                new_row[self.SCHEMA.SUBCATEGORY] = subcategory
                new_row[self.SCHEMA.SUBCATEGORY_SPLIT] = np.nan

                if float(value) > 0:
                    new_row[self.SCHEMA.IN] = value
                else:
                    new_row[self.SCHEMA.OUT] = value

                df_keep = pd.concat([df_keep, pd.DataFrame([new_row])], ignore_index=True)

        return df_keep.reset_index(drop=True)


class Budget(Finances):

    DATETIME_FORMAT: str = '%d/%m/%Y'
    SKIPROWS: int = 1
    SCHEMA: SchemaBudget = SchemaBudget()

    def preprocess(self) -> pd.DataFrame:
        '''loads budget file and returns as pandas dataframe'''

        log_file = f'inputs/budget_{self.month_id.replace(" ", "_")}.csv'
        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df[self.SCHEMA.ID] = self.add_id_column(df)
        df[self.SCHEMA.CATEGORY] = self.add_category_column(df)
        df[self.SCHEMA.BUDGET] = self.convert_to_pennies(df[self.SCHEMA.BUDGET])

        df = df[self.SCHEMA.df_columns_final]

        return df

    def add_datetime_column(self, df: pd.DataFrame) -> pd.DataFrame:
        '''adds datetime column to existing dataframe based on date row, then remove that row'''

        dt_idx = df.index[df[self.SCHEMA.SUBCATEGORY] == 'date']
        date = df.iloc[dt_idx][self.SCHEMA.BUDGET].values[0]
        df[self.SCHEMA.DATETIME] = datetime.strptime(date, self.DATETIME_FORMAT)
        df = df.drop(dt_idx, axis=0).reset_index(drop=True)

        return df

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
        assert diff == set(), f'Monzo statement contains subcategories missing from sub_category.json ({diff})'

        return df[self.SCHEMA.SUBCATEGORY].map(sub_category)


class Accounts(Finances):
    DATETIME_FORMAT: str = '%d/%m/%Y'
    SKIPROWS: int = 1
    SCHEMA: SchemaAccounts = SchemaAccounts()

    def preprocess(self) -> pd.DataFrame:
        '''loads accounts file and returns as pandas dataframe'''

        log_file = f'inputs/accounts_{self.month_id.replace(" ", "_")}.csv'
        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df[self.SCHEMA.ID] = self.add_id_column(df)
        df[self.SCHEMA.BALANCE] = self.convert_to_pennies(df[self.SCHEMA.BALANCE])

        df = df[self.SCHEMA.df_columns_final]

        return df

    def add_datetime_column(self, df: pd.DataFrame) -> pd.DataFrame:
        '''adds datetime column to existing dataframe based on date row, then remove that row'''

        dt_idx = df.index[df[self.SCHEMA.ACCOUNT] == 'date']
        date = df.iloc[dt_idx][self.SCHEMA.BALANCE].values[0]
        df[self.SCHEMA.DATETIME] = datetime.strptime(date, self.DATETIME_FORMAT)
        df = df.drop(dt_idx, axis=0).reset_index(drop=True)

        return df


class Income(Finances):
    DATETIME_FORMAT: str = '%d/%m/%Y'
    SKIPROWS: int = 1
    SCHEMA: SchemaIncome = SchemaIncome()

    def preprocess(self) -> pd.DataFrame:
        '''loads income file and returns as pandas dataframe'''

        log_file = f'inputs/income_{self.month_id.replace(" ", "_")}.csv'
        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df[self.SCHEMA.ID] = self.add_id_column(df)
        df[self.SCHEMA.AMOUNT] = self.convert_to_pennies(df[self.SCHEMA.AMOUNT])

        df = df[self.SCHEMA.df_columns_final]

        return df

    def add_datetime_column(self, df: pd.DataFrame) -> pd.DataFrame:
        '''adds datetime column to existing dataframe based on date row, then remove that row'''

        dt_idx = df.index[df[self.SCHEMA.TYPE] == 'date']
        date = df.iloc[dt_idx][self.SCHEMA.AMOUNT].values[0]
        df[self.SCHEMA.DATETIME] = datetime.strptime(date, self.DATETIME_FORMAT)
        df = df.drop(dt_idx, axis=0).reset_index(drop=True)

        return df


class InvestmentVariable(Finances):
    DATETIME_FORMAT: str = '%d/%m/%Y'
    SKIPROWS: int = 1
    SCHEMA: SchemaInvestmentVariable = SchemaInvestmentVariable()

    def preprocess(self) -> pd.DataFrame:
        '''loads investments_variable file and returns as pandas dataframe'''

        log_file = f'inputs/investments_variable_{self.month_id.replace(" ", "_")}.csv'
        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df[self.SCHEMA.DATETIME] = self.add_datetime_column(df)
        df[self.SCHEMA.MONTH_ID] = self.add_month_id_column(df)
        df[self.SCHEMA.ID] = self.add_id_column(df)
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


class InvestmentFixed(Finances):
    SKIPROWS: int = 1
    SCHEMA: SchemaInvestmentFixed = SchemaInvestmentFixed()

    # TODO: only need to of duration, purchase date and maturity date. Should calculate the 3rd
    def preprocess(self) -> pd.DataFrame:
        '''loads investments_fixed file and returns as pandas dataframe'''

        log_file = 'inputs/investments_fixed.csv'
        df = pd.read_csv(log_file, skiprows=self.SKIPROWS, names=self.SCHEMA.df_columns_initial)

        df[self.SCHEMA.AMOUNT] = self.convert_to_pennies(df[self.SCHEMA.AMOUNT])
        df[self.SCHEMA.RETURN] = self.add_return_column(df)
        df[self.SCHEMA.ID] = self.add_id_column(df)

        df = df[self.SCHEMA.df_columns_final]

        return df

    def add_return_column(self, df: pd.DataFrame) -> pd.Series:

        df['duration_yrs'] = df[self.SCHEMA.DURATION]//12 + (df[self.SCHEMA.DURATION]%12)/12
        rtn = df['duration_yrs'] * df[self.SCHEMA.AMOUNT] * (df[self.SCHEMA.INTEREST]/100)
        return rtn.astype(int)

    def add_id_column(self, df: pd.DataFrame) -> pd.Series:
        return df[self.SCHEMA.NAME].astype(str) + df[self.SCHEMA.COMPANY].astype(str) + df[self.SCHEMA.AMOUNT].astype(str) + df[self.SCHEMA.INTEREST].astype(str) + df[self.SCHEMA.DURATION].astype(str) + df[self.SCHEMA.MATURITY_DATE].astype(str)

