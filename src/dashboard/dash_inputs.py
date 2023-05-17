from sqlalchemy import inspect, select, Table, and_
import pandas as pd
from src.preprocessing.db_manager import SQL, get_class_from_table_name, InvestmentsVariableTbl, InvestmentsFixedTbl
from datetime import datetime
from src.preprocessing.api import SchemaMonzo, SchemaBudget, SchemaAccounts, SchemaIncome, SchemaInvestmentFixed, SchemaInvestmentVariable

import logging

'''
this script will contain the functions to generate all tables and constants that are then displayed in the dashboard.
'''

def query_db(table_name, month_id):

    db = SQL()

    assert table_name in inspect(db.engine).get_table_names(), f'{table_name} is not a valid table name.'

    class_ = get_class_from_table_name(table_name)
    table = Table(table_name, class_.metadata)

    with db.engine.connect() as conn:
        query = select(table).where(table.c.month_id == month_id)
        df = pd.read_sql(sql=query, con=conn)

    return df


def query_inv_var(month_id):

    dt_month_id = datetime.strptime(month_id.lower(), '%b %y')
    dt_previous_month = dt_month_id - pd.DateOffset(months=1)
    previous_month_id = datetime.strftime(dt_previous_month, '%b %y').upper()

    SCHEMA = SchemaInvestmentVariable()
    db = SQL()

    table = Table(InvestmentsVariableTbl.__tablename__, InvestmentsVariableTbl.metadata)

    with db.engine.connect() as conn:
        query = select(table).where(table.c.month_id.like(month_id))
        inv = pd.read_sql(sql=query, con=conn)

    with db.engine.connect() as conn:
        query = select(table).where(table.c.month_id.like(previous_month_id))
        inv_previous = pd.read_sql(sql=query, con=conn)

    inv_var = inv.merge(inv_previous, on=SCHEMA.NAME, how='left', suffixes=('', '_prev'))

    return inv_var

def query_inv_fix(month_id):

    dt_month_id = datetime.strptime(month_id.lower(), '%b %y')

    db = SQL()

    table = Table(InvestmentsFixedTbl.__tablename__, InvestmentsFixedTbl.metadata)
    with db.engine.connect() as conn:
        current_date = dt_month_id + pd.DateOffset(months=1)
        # TODO: query currently not working
        query = select(table).where((table.c.Matures > current_date) & (table.c.Purchased < current_date))
        # query = select(table).filter(and_(table.c.Matures > current_date, table.c.Purchased < current_date))
        inv_fix = pd.read_sql(sql=query, con=conn)

    return inv_fix


def spending_table(month_id: str, dd_mm: bool) -> pd.DataFrame:

    SCHEMA = SchemaMonzo()
    df = query_db('spending', month_id)

    miscellaneous_categories = ["General", "Charity", "Expenses", "Savings", "Transfers", "Family", "Finances"]
    for category in miscellaneous_categories:
        if df[df[SCHEMA.SUBCATEGORY]==category].shape[0] != 0:
            logging.warning(f'Monzo data contains {df[df[SCHEMA.SUBCATEGORY] == category].shape[0]} transactions categorised as {category}. These will be removed.')
    df = df[~df[SCHEMA.SUBCATEGORY].isin(miscellaneous_categories)]

    df = df.sort_values(SCHEMA.DATETIME)
    df[SCHEMA.BALANCE] = df[SCHEMA.IN].cumsum() + df[SCHEMA.OUT].cumsum()
    df[[SCHEMA.BALANCE, SCHEMA.IN, SCHEMA.OUT]] /= 100  # convert from int pennies to £'s
    df[SCHEMA.OUT] = -df[SCHEMA.OUT]
    if dd_mm:
        df[SCHEMA.DATETIME] = df[SCHEMA.DATETIME].apply(lambda x: datetime.strftime(x, "%d/%m"))

    df = df[[SCHEMA.DATETIME, SCHEMA.SUBCATEGORY, SCHEMA.NAME, SCHEMA.DESCRIPTION, SCHEMA.IN, SCHEMA.OUT, SCHEMA.BALANCE]]

    return df.reset_index(drop=True)

def summary_table(month_id: str, total_row: bool) -> tuple[pd.DataFrame, float, float]:

    SCHEMA = SchemaMonzo()
    SCHEMABudget = SchemaBudget()

    # import dfs from database
    df = spending_table(month_id, False)
    df_budget = query_db('budget', month_id)
    # merge dfs on subcategory column
    df = df.groupby(SCHEMA.SUBCATEGORY)[[SCHEMA.IN, SCHEMA.OUT]].sum().reset_index()
    df = df_budget[[SCHEMABudget.SUBCATEGORY, SCHEMABudget.CATEGORY, SCHEMABudget.BUDGET]].merge(df, how='left', on=SCHEMA.SUBCATEGORY)
    # add difference and total columns
    df = df.fillna(0)
    df[SCHEMA.OUT] = -df[SCHEMA.OUT]
    df[SCHEMA.TOTAL] = df[SCHEMA.IN] + df[SCHEMA.OUT]
    # convert from int pennies to £'s
    df[SCHEMABudget.BUDGET] /= 100
    df[SCHEMA.DIFFERENCE] = df[SCHEMABudget.BUDGET] + df[SCHEMA.TOTAL]


    # remove 'miscellaneous' rows and order rows
    df = df[df[SCHEMA.CATEGORY] != 'Miscellaneous']
    df = df[df[SCHEMA.SUBCATEGORY] != 'Bills']
    subcategory_order = pd.CategoricalDtype(['Income',
                                             'Transport', 'Car',
                                             'Groceries', 'Snacks', 'Lunch', 'Eating out', 'Alcohol',
                                             'Shopping', 'Clothes', 'Electronics', 'Personal care', 'Gifts',
                                             'Entertainment',
                                             'Holidays',
                                             'Bills'],
                                            ordered=True)
    df[SCHEMA.SUBCATEGORY] = df[SCHEMA.SUBCATEGORY].astype(subcategory_order)
    df = df.sort_values(SCHEMA.SUBCATEGORY).reset_index(drop=True)

    # filter to final columns
    df = df[[SCHEMA.CATEGORY, SCHEMA.SUBCATEGORY, SCHEMA.IN, SCHEMA.OUT, SCHEMA.TOTAL, SCHEMABudget.BUDGET, SCHEMA.DIFFERENCE]]

    # calculate constants
    monthly_budget = df[~df[SCHEMA.SUBCATEGORY].isin(['Income', 'Bills'])][SCHEMABudget.BUDGET].sum()
    monthly_spending = df[~df[SCHEMA.SUBCATEGORY].isin(['Income', 'Bills'])][SCHEMA.TOTAL].sum()

    if total_row:
        total = {'Subcategory': 'TOTAL',
                 'In': df[SCHEMA.IN].sum(),
                 'Out': df[SCHEMA.OUT].sum(),
                 'Total': df[SCHEMA.TOTAL].sum(),
                 'Budget': df[SCHEMABudget.BUDGET].sum(),
                 'Diff.': df[SCHEMA.DIFFERENCE].sum()}
        df.loc[len(df)] = ['TOTAL', 'TOTAL', df[SCHEMA.IN].sum(), df[SCHEMA.OUT].sum(), df[SCHEMA.TOTAL].sum(),
                           df[SCHEMABudget.BUDGET].sum(), df[SCHEMA.DIFFERENCE].sum()]

    return df, monthly_budget, monthly_spending

def accounts_table(month_id: str) -> tuple[pd.DataFrame, float]:

    SCHEMA = SchemaAccounts()

    df = query_db('accounts', month_id)

    df[SCHEMA.BALANCE] /= 100  # convert from int pennies to £'s
    df = df[[SCHEMA.ACCOUNT, SCHEMA.BALANCE]]

    liquidity = df[SCHEMA.BALANCE].sum()

    return df, liquidity

def income_table(month_id: str) -> pd.DataFrame:

    SCHEMA = SchemaIncome()

    df = query_db('income', month_id)

    df[SCHEMA.AMOUNT] /= 100  # convert from int pennies to £'s
    df = df[[SCHEMA.TYPE, SCHEMA.AMOUNT]]

    return df

def investment_tables(month_id, liquidity) -> tuple[pd.DataFrame, pd.DataFrame, float]:

    SCHEMAVar = SchemaInvestmentVariable()

    inv_var = query_inv_var(month_id)

    inv_var[SCHEMAVar.D_UNIT_PRICE] = (100 * (inv_var[SCHEMAVar.UNIT_PRICE] - inv_var[SCHEMAVar.UNIT_PRICE_PREV]) / inv_var[SCHEMAVar.UNIT_PRICE_PREV]).round(2)
    inv_var[SCHEMAVar.D_VALUE] = (inv_var[SCHEMAVar.VALUE] - inv_var[SCHEMAVar.VALUE_PREV]).round(2)
    inv_var = inv_var[[SCHEMAVar.NAME, SCHEMAVar.UNIT_PRICE, SCHEMAVar.D_UNIT_PRICE, SCHEMAVar.UNITS_OWNED, SCHEMAVar.VALUE, SCHEMAVar.D_VALUE]]

    SCHEMAFix = SchemaInvestmentFixed()

    inv_fix = query_inv_fix(month_id)

    inv_fix[[SCHEMAFix.AMOUNT, SCHEMAFix.RETURN]] /= 100  # convert from int pennies to £'s
    inv_fix = inv_fix.sort_values(SCHEMAFix.MATURITY_DATE)
    inv_fix = inv_fix[[SCHEMAFix.NAME, SCHEMAFix.COMPANY, SCHEMAFix.AMOUNT, SCHEMAFix.INTEREST, SCHEMAFix.RETURN, SCHEMAFix.DURATION, SCHEMAFix.MATURITY_DATE]]

    net_worth = liquidity + inv_var[SCHEMAVar.VALUE].sum() + inv_fix[SCHEMAFix.AMOUNT].sum()

    return inv_var, inv_fix, round(net_worth, 2)

if __name__ == '__main__':

    month_id = 'JAN 23'
    df = spending_table(month_id, False)
    inv_f, inv_v, _ = investment_tables(month_id, 1000)
