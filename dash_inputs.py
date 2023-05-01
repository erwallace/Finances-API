from sqlalchemy import text
import pandas as pd
from sql import SQL
from datetime import datetime

'''
this script will contain the functions to generate all tables and constants that are then displayed in the dashboard.
'''


def query_db(table_name, month_id):

    db = SQL()

    with db.engine.connect() as conn:
        query = text(f'SELECT * FROM {table_name} WHERE month_id LIKE :m_ids')

        df = pd.read_sql(sql=query,
                          con=conn,
                          params={"m_ids": month_id}
                          )

    return df


def query_inv_var(month_id):

    dt_month_id = datetime.strptime(month_id.lower(), '%b %y')
    dt_previous_month = dt_month_id - pd.DateOffset(months=1)
    previous_month_id = datetime.strftime(dt_previous_month, '%b %y').upper()

    db = SQL()

    with db.engine.connect() as conn:
        query = text("""SELECT *
                        FROM investments_variable
                        WHERE month_id LIKE :m_ids""")

        inv = pd.read_sql(sql=query,
                          con=conn,
                          params={"m_ids": month_id})

    with db.engine.connect() as conn:
        query = text("""SELECT *
                        FROM investments_variable
                        WHERE month_id LIKE :m_ids""")

        inv_previous = pd.read_sql(sql=query,
                                   con=conn,
                                   params={"m_ids": previous_month_id})

    inv_var = inv.merge(inv_previous, on='name', how='left', suffixes=('', '_prev'))

    return inv_var

def query_inv_fix(month_id):

    dt_month_id = datetime.strptime(month_id.lower(), '%b %y')

    db = SQL()

    with db.engine.connect() as conn:
        query = text("""SELECT *
                        FROM investments_fixed
                        WHERE maturity_date > :current_date
                            AND purchase_date < :current_date""")

        inv_fix = pd.read_sql(sql=query,
                              con=conn,
                              params={"current_date": dt_month_id + pd.DateOffset(months=1)})

    return inv_fix


def all_spending_table(spending_data_df: pd.DataFrame) -> pd.DataFrame:

    spending = spending_data_df[['date', 'subcategory', 'description', 'in', 'out']]
    # TODO: check that description has everything I want in it
    spending.loc[:, ['in', 'out']] = spending[['in', 'out']].fillna(0)
    spending['balance'] = spending['in'].cumsum() + spending['out'].cumsum()
    # TODO: shorten date to dd/mm/yy
    return spending


def summary_spending_table(spending_data_df: pd.DataFrame, budget_df: pd.DataFrame) -> pd.DataFrame:

    summary = spending_data_df.groupby('subcategory')[['in', 'out']].sum().reset_index()
    summary = budget_df[['subcategory', 'category', 'budget']].merge(summary, how='left', on='subcategory')

    summary['total'] = summary['in'] + summary['out']
    summary['diff'] = summary['budget'] + summary['total']
    summary = summary.fillna(0)

    summary = summary[summary.category != 'Miscellaneous']

    subcategory_order = pd.CategoricalDtype(['Income',
                                             'Transport', 'Car',
                                             'Groceries', 'Snacks', 'Lunch', 'Eating out', 'Alcohol',
                                             'Shopping', 'Clothes', 'Electronics', 'Personal care', 'Gifts',
                                             'Entertainment',
                                             'Holidays',
                                             'Bills'],
                                            ordered=True)
    summary.subcategory = summary.subcategory.astype(subcategory_order)
    summary = summary.sort_values('subcategory').reset_index(drop=True)

    summary = summary[['category', 'subcategory', 'in', 'out', 'total', 'budget', 'diff']]

    return summary


def income_table(income_df: pd.DataFrame) -> pd.DataFrame:

    return income_df[['type', 'amount']]


def accounts_table(accounts_df: pd.DataFrame) -> pd.DataFrame:

    return accounts_df[['account', 'balance']]


def inv_var_table(inv_var_df: pd.DataFrame) -> pd.DataFrame:

    inv_var_df['d%_unit_price'] = (100 * (inv_var_df.unit_price - inv_var_df.unit_price_prev) / inv_var_df.unit_price_prev).round(2)
    inv_var_df['d_value'] = (inv_var_df.value - inv_var_df.value_prev).round(2)
    inv_var_df = inv_var_df[['name', 'unit_price', 'd%_unit_price', 'units_owned', 'value', 'd_value']]

    return inv_var_df


def inv_fix_table():
    pass


def get_monthly_budget(spending_data_df: pd.DataFrame, budget_df: pd.DataFrame) -> float:

    summary = summary_spending_table(spending_data_df, budget_df)
    return summary[~summary.subcategory.isin(['Income', 'Bills'])].budget.sum()


def get_monthly_spend(spending_data_df: pd.DataFrame, budget_df: pd.DataFrame) -> float:

    summary = summary_spending_table(spending_data_df, budget_df)
    return summary[~summary.subcategory.isin(['Income', 'Bills'])].total.sum()





