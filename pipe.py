import argparse
from db_manager import SQL
from api import *

import logging
from log import get_logger
from sqlalchemy import text
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

logger = get_logger(__name__)

class pipeline:

    @staticmethod
    def create_tables_in_db():
        sql = SQL()
        sql.create_all_tables()

    @staticmethod
    def delete_all_db_tables():
        sql = SQL()
        sql.delete_all_tables()

    @staticmethod
    def append_to_db(month_id: str):
        sql = SQL()

        mz = Monzo(month_id)
        df_mz, months = mz.preprocess()
        sql.append_to_db(months, 'months')
        sql.append_to_db(df_mz, 'spending')

        bud = Budget(month_id)
        df_bud = bud.preprocess()
        sql.append_to_db(df_bud, 'budget')

        acc = Accounts(month_id)
        df_acc = acc.preprocess()
        sql.append_to_db(df_acc, 'accounts')

        inc = Income(month_id)
        df_inc = inc.preprocess()
        sql.append_to_db(df_inc, 'income')

        inv = InvestmentVariable(month_id)
        df_inv = inv.preprocess()
        sql.append_to_db(df_inv, 'investments_variable')

        inf = InvestmentFixed(month_id)
        df_inf = inf.preprocess()
        sql.append_to_db(df_inf, 'investments_fixed')

    @staticmethod
    def generate_dashboard():
        pass

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--month", required=True)  # month
    parser.add_argument("-y", "--year", required=True)  # year
    parser.add_argument("-c", "--create", action=argparse.BooleanOptionalAction)  # create db tables?
    parser.add_argument("-a", "--append", action=argparse.BooleanOptionalAction)  # append to db?
    parser.add_argument("-d", "--dashboard", action=argparse.BooleanOptionalAction)  # generate dashboard?
    args = parser.parse_args()

    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    assert args.month[:3].upper() in months, f'"{args.month[:3].upper()}" is not a valid month.'

    month_id = f'{args.month[:3].upper()} {args.year[-2:]}'
    logging.info(f'month_id is "{month_id}"')

    if args.create:
        pipeline.create_tables_in_db()
    if args.append:
        pipeline.append_to_db(month_id)
    if args.dashboard:
        pipeline.generate_dashboard()

