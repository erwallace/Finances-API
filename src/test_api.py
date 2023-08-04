import pytest
import os
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal, assert_series_equal
from datetime import datetime
import os

from api import Finances, SchemaMonzo, Monzo, SchemaInputs, Budget, Accounts, Income, SchemaInvestmentVariable, InvestmentVariable, SchemaInvestmentFixed, InvestmentFixed


class TestFinancesPreprocessing:

    SCHEMA = SchemaMonzo()
    month_id = 'FEB 23'
    mz = Monzo(month_id)
    bud = Budget(month_id)

    @pytest.fixture
    def input_data(self):
        os.environ['DEBUG'] = 'True'
        yield pd.DataFrame.from_dict({self.SCHEMA.ID: ['tx_0000ASo4xSWfgUfqodsowj', 'tx_0000ASZejZR9sIU90l4gss'],
                                      self.SCHEMA.DATE: ['18/02/2023', '11/02/2023'],
                                      self.SCHEMA.TIME: ['14:32:44', '15:32:56'],
                                      self.SCHEMA.TYPE: ['Card payment', 'Faster payment'],
                                      self.SCHEMA.NAME: ['Tesco', 'Ikran Jama'],
                                      self.SCHEMA.EMOJI: ['ðŸ', np.nan],
                                      self.SCHEMA.SUBCATEGORY: ['Groceries', 'Entertainment'],
                                      self.SCHEMA.AMOUNT: ['-19.51', '5'],
                                      self.SCHEMA.CURRENCY: ['GBP', 'GBP'],
                                      self.SCHEMA.LOCAL_AMOUNT: ['-19.51', '5'],
                                      self.SCHEMA.LOCAL_CURRENCY: ['GBP', 'GBP'],
                                      self.SCHEMA.NOTES: [np.nan, 'bop ticket'],
                                      self.SCHEMA.ADDRESS: ['159-161 Cowley Road', np.nan],
                                      self.SCHEMA.RECEIPT: [np.nan, np.nan],
                                      self.SCHEMA.DESCRIPTION: ['TESCO STORES 2990      OXFORD 1      GBR', 'bop ticket'],
                                      self.SCHEMA.SUBCATEGORY_SPLIT: ['Alcohol:-7.00,Groceries:-12.51', np.nan],
                                      self.SCHEMA.OUT: ['-19.51', np.nan],
                                      self.SCHEMA.IN: [np.nan, '5']
                                      })

    def test_add_datetime_column(self, input_data: pytest.fixture):
        ''' tests the add_datetime_column() method of Finances '''
        input_data[self.SCHEMA.DATETIME] = self.bud.add_datetime_column(input_data, self.month_id)
        expected = pd.Series({0: datetime(2023, 2, 28), 1: datetime(2023, 2, 28)}, name=self.SCHEMA.DATETIME)

        assert_series_equal(input_data[self.SCHEMA.DATETIME], expected)

    def test_add_month_id_column(self, input_data: pytest.fixture):
        ''' tests the add_month_id_column() method of Finances. This method required the Date column to be populated
        with datetime objects '''
        input_data[self.SCHEMA.DATETIME] = self.mz.add_datetime_column(input_data)
        input_data[self.SCHEMA.MONTH_ID] = self.mz.add_month_id_column(input_data)
        expected = pd.Series({0: 'FEB 23', 1: 'FEB 23'}, name=self.SCHEMA.MONTH_ID)

        assert_series_equal(input_data[self.SCHEMA.MONTH_ID], expected)

    def test_add_category_column(self, input_data: pytest.fixture):
        ''' tests the add_category_column() method of Finances '''
        input_data[self.SCHEMA.CATEGORY] = self.mz.add_category_column(input_data)
        expected = pd.Series({0: 'Food & Drink', 1: 'Entertainment'}, name=self.SCHEMA.CATEGORY)

        assert_series_equal(input_data[self.SCHEMA.CATEGORY], expected)

    def test_convert_to_pennies(self, input_data: pytest.fixture):
        ''' tests the convert_to_pennies() method of Finances '''
        input_data[self.SCHEMA.OUT] = self.mz.convert_to_pennies(input_data[self.SCHEMA.OUT])
        expected = pd.Series({0: -1951, 1: 0}, name=self.SCHEMA.OUT)

        assert_series_equal(input_data[self.SCHEMA.OUT], expected.astype(int))

    def test_add_id_column(self, input_data: pytest.fixture):
        ''' tests the add_id_column() method of Finances. This method required the month_id column to be populated '''
        input_data[self.SCHEMA.DATETIME] = self.mz.add_datetime_column(input_data)
        input_data[self.SCHEMA.MONTH_ID] = self.mz.add_month_id_column(input_data)
        input_data[self.SCHEMA.ID] = self.mz.add_id_column(input_data)
        expected = pd.Series({0: 'FEB 23 0000', 1: 'FEB 23 0001'}, name=self.SCHEMA.ID)

        assert_series_equal(input_data[self.SCHEMA.ID], expected)


class TestMonzoPreprocessing:

    SCHEMA = SchemaMonzo()
    month_id = 'FEB 23'
    mz = Monzo(month_id)

    @pytest.fixture
    def input_data(self):
        os.environ['DEBUG'] = 'True'
        yield pd.DataFrame.from_dict({self.SCHEMA.ID: ['tx_0000ASo4xSWfgUfqodsowj', 'tx_0000ASZejZR9sIU90l4gss'],
                                      self.SCHEMA.DATE: ['18/02/2023', '11/02/2023'],
                                      self.SCHEMA.TIME: ['14:32:44', '15:32:56'],
                                      self.SCHEMA.TYPE: ['Card payment', 'Faster payment'],
                                      self.SCHEMA.NAME: ['Tesco', 'Ikran Jama'],
                                      self.SCHEMA.EMOJI: ['ðŸ', np.nan],
                                      self.SCHEMA.SUBCATEGORY: ['Groceries', 'Entertainment'],
                                      self.SCHEMA.AMOUNT: ['-19.51', '5'],
                                      self.SCHEMA.CURRENCY: ['GBP', 'GBP'],
                                      self.SCHEMA.LOCAL_AMOUNT: ['-19.51', '5'],
                                      self.SCHEMA.LOCAL_CURRENCY: ['GBP', 'GBP'],
                                      self.SCHEMA.NOTES: [np.nan, 'bop ticket'],
                                      self.SCHEMA.ADDRESS: ['159-161 Cowley Road', np.nan],
                                      self.SCHEMA.RECEIPT: [np.nan, np.nan],
                                      self.SCHEMA.DESCRIPTION: ['TESCO STORES 2990      OXFORD 1      GBR', 'bop ticket'],
                                      self.SCHEMA.SUBCATEGORY_SPLIT: ['Alcohol:-7.00,Groceries:-12.51', np.nan],
                                      self.SCHEMA.OUT: ['-19.51', np.nan],
                                      self.SCHEMA.IN: [np.nan, '5']
                                      })

    def test_add_datetime_column(self, input_data: pytest.fixture):
        ''' tests the add_datetime_column() method of Monzo '''
        input_data[self.SCHEMA.DATETIME] = self.mz.add_datetime_column(input_data)
        expected = pd.Series({0:datetime(2023, 2, 18, 14, 32, 44), 1:datetime(2023, 2, 11, 15, 32, 56)}, name=self.SCHEMA.DATETIME)

        assert_series_equal(input_data[self.SCHEMA.DATETIME], expected)

    def test_split_subcategory_payments(self, input_data: pytest.fixture):
        ''' tests the split_subcategory_payments() method of Monzo '''
        df = self.mz.split_subcategory_payments(input_data)
        expected = pd.DataFrame.from_dict({self.SCHEMA.ID: ['tx_0000ASZejZR9sIU90l4gss', 'tx_0000ASo4xSWfgUfqodsowj', 'tx_0000ASo4xSWfgUfqodsowj'],
                                      self.SCHEMA.DATE: ['11/02/2023', '18/02/2023', '18/02/2023'],
                                      self.SCHEMA.TIME: ['15:32:56', '14:32:44', '14:32:44'],
                                      self.SCHEMA.TYPE: ['Faster payment', 'Card payment', 'Card payment'],
                                      self.SCHEMA.NAME: ['Ikran Jama', 'Tesco', 'Tesco'],
                                      self.SCHEMA.EMOJI: [np.nan, 'ðŸ', 'ðŸ'],
                                      self.SCHEMA.SUBCATEGORY: ['Entertainment', 'Alcohol', 'Groceries'],
                                      self.SCHEMA.AMOUNT: ['5', '-7.00', '-12.51'],
                                      self.SCHEMA.CURRENCY: ['GBP', 'GBP', 'GBP'],
                                      self.SCHEMA.LOCAL_AMOUNT: ['5', '-7.00', '-12.51'],
                                      self.SCHEMA.LOCAL_CURRENCY: ['GBP', 'GBP', 'GBP'],
                                      self.SCHEMA.NOTES: ['bop ticket', np.nan, np.nan],
                                      self.SCHEMA.ADDRESS: [np.nan, '159-161 Cowley Road', '159-161 Cowley Road'],
                                      self.SCHEMA.RECEIPT: [np.nan, np.nan, np.nan],
                                      self.SCHEMA.DESCRIPTION: ['bop ticket', 'TESCO STORES 2990      OXFORD 1      GBR', 'TESCO STORES 2990      OXFORD 1      GBR'],
                                      self.SCHEMA.SUBCATEGORY_SPLIT: [np.nan, np.nan, np.nan],
                                      self.SCHEMA.OUT: [np.nan, '-7.00', '-12.51'],
                                      self.SCHEMA.IN: ['5', np.nan, np.nan]
                                      })

        assert_frame_equal(df, expected, check_dtype=False)

    def test_preprocess_monzo(self, input_data: pytest.fixture):
        ''' tests the first output (df) of the preprocess() method of Monzo '''
        df, _ = self.mz.preprocess(DEBUG=input_data)
        expected = pd.DataFrame.from_dict({self.SCHEMA.ID: ['FEB 23 0000', 'FEB 23 0001', 'FEB 23 0002'],
                                           self.SCHEMA.MONTH_ID: ['FEB 23', 'FEB 23', 'FEB 23'],
                                           self.SCHEMA.DATETIME: [datetime(2023, 2, 11, 15, 32, 56), datetime(2023, 2, 18, 14, 32, 44), datetime(2023, 2, 18, 14, 32, 44)],
                                           self.SCHEMA.TYPE: ['Faster payment', 'Card payment', 'Card payment'],
                                           self.SCHEMA.NAME: ['Ikran Jama', 'Tesco', 'Tesco'],
                                           self.SCHEMA.CATEGORY: ['Entertainment', 'Food & Drink', 'Food & Drink'],
                                           self.SCHEMA.SUBCATEGORY: ['Entertainment', 'Alcohol', 'Groceries'],
                                           self.SCHEMA.ADDRESS: [np.nan, '159-161 Cowley Road', '159-161 Cowley Road'],
                                           self.SCHEMA.DESCRIPTION: ['bop ticket', 'TESCO STORES 2990      OXFORD 1      GBR', 'TESCO STORES 2990      OXFORD 1      GBR'],
                                           self.SCHEMA.OUT: [0, -700, -1251],
                                           self.SCHEMA.IN: [500, 0, 0]
                                           })

        assert_frame_equal(df, expected, check_dtype=False)

    def test_preprocess_months(self, input_data: pytest.fixture):
        ''' tests the second output (months) of the preprocess() method of Monzo '''
        _, months = self.mz.preprocess(DEBUG=input_data)
        expected = pd.DataFrame.from_dict({self.SCHEMA.ID: ['FEB 23'],
                                           self.SCHEMA.DATETIME: [datetime(2023, 2, 1)]
                                           })

        assert_frame_equal(months, expected, check_dtype=False)


class TestInputsPreprocessing:

    SCHEMA = SchemaInputs()
    month_id = 'FEB 23'

    @pytest.fixture
    def input_data(self):
        os.environ['DEBUG'] = 'True'
        yield pd.DataFrame([{self.SCHEMA.CATEGORY: 'BUDGET',
                             self.SCHEMA.SUBCATEGORY: 'Lunch',
                             self.SCHEMA.AMOUNT: 80.0,
                             self.SCHEMA.COMMENT: np.nan},
                            {self.SCHEMA.CATEGORY: 'INCOME',
                             self.SCHEMA.SUBCATEGORY: 'Paycheck',
                             self.SCHEMA.AMOUNT: 2000.0,
                             self.SCHEMA.COMMENT: 'positive'},
                            {self.SCHEMA.CATEGORY: 'ACCOUNTS',
                             self.SCHEMA.SUBCATEGORY: 'Monzo: Current',
                             self.SCHEMA.AMOUNT: 34.56,
                             self.SCHEMA.COMMENT: np.nan},
                            ])

    def test_preprocess_budget(self, input_data: pytest.fixture):
        ''' tests the preprocess() method of Budget '''
        bud = Budget(self.month_id)
        df = bud.preprocess(input_data)
        expected = pd.DataFrame([{self.SCHEMA.ID: 'FEB 23 0000',
                                  self.SCHEMA.MONTH_ID: 'FEB 23',
                                  self.SCHEMA.DATETIME: datetime(2023, 2, 28),
                                  self.SCHEMA.CATEGORY: 'Food & Drink',
                                  self.SCHEMA.SUBCATEGORY: 'Lunch',
                                  self.SCHEMA.BUDGET: 8000}])

        assert_frame_equal(df, expected, check_dtype=False)

    def test_preprocessing_accounts(self, input_data: pytest.fixture):
        ''' tests the preprocess() method of Accounts '''
        acc = Accounts(self.month_id)
        df = acc.preprocess(input_data)
        expected = pd.DataFrame([{self.SCHEMA.ID: 'FEB 23 0000',
                                  self.SCHEMA.ACCOUNT: 'Monzo: Current',
                                  self.SCHEMA.DATETIME: datetime(2023, 2, 28),
                                  self.SCHEMA.MONTH_ID: 'FEB 23',
                                  self.SCHEMA.BALANCE: 3456}])

        assert_frame_equal(df, expected, check_dtype=False)

    def test_preprocessing_income(self, input_data: pytest.fixture):
        ''' tests the preprocess() method of Income '''
        inc = Income(self.month_id)
        df = inc.preprocess(input_data)
        expected = pd.DataFrame([{self.SCHEMA.ID: 'FEB 23 0000',
                                  self.SCHEMA.TYPE: 'Paycheck',
                                  self.SCHEMA.DATETIME: datetime(2023, 2, 28),
                                  self.SCHEMA.MONTH_ID: 'FEB 23',
                                  self.SCHEMA.AMOUNT: 200000}])

        assert_frame_equal(df, expected, check_dtype=False)


class TestInvestmentVariablePreprocessing:

    SCHEMA = SchemaInvestmentVariable()
    month_id = 'FEB 23'
    inv = InvestmentVariable(month_id)

    @pytest.fixture
    def input_data(self):
        os.environ['DEBUG'] = 'True'
        yield pd.DataFrame([{self.SCHEMA.NAME: 'Vanguard LifeStrategy 60% Equity A Inc',
                             self.SCHEMA.COMPANY: 'AJBell',
                             self.SCHEMA.UNIT_PRICE: 182.2701,
                             self.SCHEMA.UNITS_OWNED: 5
                             }])

    def test_add_value_columns(self, input_data: pytest.fixture):
        ''' tests the add_value_columns() method of InvestmentVariable '''
        input_data[self.SCHEMA.DATETIME] = self.inv.add_datetime_column(input_data, self.month_id)
        expected = pd.Series({0: datetime(2023, 2, 28)}, name=self.SCHEMA.DATETIME)

        assert_series_equal(input_data[self.SCHEMA.DATETIME], expected)

    def test_preprocessing_investment_variable(self, input_data: pytest.fixture):
        ''' tests the preprocess() method of InvestmentVariable '''
        df = self.inv.preprocess(input_data)
        expected = pd.DataFrame([{self.SCHEMA.ID: 'FEB 23 0000',
                                  self.SCHEMA.NAME: 'Vanguard LifeStrategy 60% Equity A Inc',
                                  self.SCHEMA.DATETIME: datetime(2023, 2, 28),
                                  self.SCHEMA.MONTH_ID: 'FEB 23',
                                  self.SCHEMA.COMPANY: 'AJBell',
                                  self.SCHEMA.UNIT_PRICE: 182.2701,
                                  self.SCHEMA.UNITS_OWNED: 5,
                                  self.SCHEMA.VALUE: 911.3505
                                 }])

        assert_frame_equal(df, expected)


class TestInvestmentFixedPreprocessing:

    SCHEMA = SchemaInvestmentFixed()
    month_id = 'FEB 23'
    inf = InvestmentFixed(month_id)

    @pytest.fixture
    def input_data(self):
        os.environ['DEBUG'] = 'True'
        yield pd.DataFrame([{self.SCHEMA.NAME: 'Brown Shipley',
                             self.SCHEMA.COMPANY: 'AJBell',
                             self.SCHEMA.AMOUNT: 1000,
                             self.SCHEMA.INTEREST: 5.0,
                             self.SCHEMA.DURATION: 12,
                             self.SCHEMA.PURCHASE_DATE: '01/01/23',
                             self.SCHEMA.MATURITY_DATE: '01/01/24'}
                            ])

    def test_add_return_column(self, input_data: pytest.fixture):
        ''' tests the add_return_column() method of InvestmentVariable '''
        input_data[self.SCHEMA.AMOUNT] = self.inf.convert_to_pennies(input_data[self.SCHEMA.AMOUNT])
        input_data[self.SCHEMA.RETURN] = self.inf.add_return_column(input_data)
        expected = pd.Series({0: 5000}, name=self.SCHEMA.RETURN)

        assert_series_equal(input_data[self.SCHEMA.RETURN], expected, check_dtype=False)

    def test_add_id_column(self, input_data: pytest.fixture):
        ''' tests the add_id_column() method of InvestmentVariable '''
        input_data[self.SCHEMA.ID] = self.inf.add_id_column(input_data)
        expected = pd.Series({0: 'Brown ShipleyAJBell10005.01201/01/24'}, name=self.SCHEMA.ID)

        print(input_data[self.SCHEMA.ID])

        assert_series_equal(input_data[self.SCHEMA.ID], expected)

    def test_preprocessing_investment_variable(self, input_data: pytest.fixture):
        ''' tests the preprocess() method of InvestmentVariable '''
        df = self.inf.preprocess(input_data)
        expected = pd.DataFrame([{self.SCHEMA.ID: 'Brown ShipleyAJBell1000005.01201/01/24',
                                  self.SCHEMA.NAME: 'Brown Shipley',
                                  self.SCHEMA.COMPANY: 'AJBell',
                                  self.SCHEMA.AMOUNT: 100000,
                                  self.SCHEMA.INTEREST: 5.0,
                                  self.SCHEMA.DURATION: 12,
                                  self.SCHEMA.PURCHASE_DATE: '01/01/23',
                                  self.SCHEMA.MATURITY_DATE: '01/01/24',
                                  self.SCHEMA.RETURN: 5000
                                  }])

        assert_frame_equal(df, expected, check_dtype=False)


