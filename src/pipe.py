import argparse
from sql.db_manager import SQL
from api import *
from dashboard.dashboard import my_dashboard

import logging
from utils.log import get_logger
logger = get_logger(__name__)


class pipeline:

    def __init__(self):

        self.db = SQL()

    def create_tables_in_db(self) -> None:
        ''' creates all tables in the schema '''
        self.db.create_all_tables()

    def delete_all_db_tables(self) -> None:
        ''' deletes all tables from the schema '''
        self.db.delete_all_tables()

    def append_to_db(self, month_id: str, demo: bool) -> None:
        ''' iterate through all tables in schema, appending all non-duplicate rows'''
        mz = Monzo(month_id)
        df_mz, months = mz.preprocess(demo=demo)
        self.db.append_to_db(months, 'months')
        self.db.append_to_db(df_mz, 'spending')

        bud = Budget(month_id)
        df_bud = bud.preprocess(demo=demo)
        self.db.append_to_db(df_bud, 'budget')

        acc = Accounts(month_id)
        df_acc = acc.preprocess(demo=demo)
        self.db.append_to_db(df_acc, 'accounts')

        inc = Income(month_id)
        df_inc = inc.preprocess(demo=demo)
        self.db.append_to_db(df_inc, 'income')

        inv = InvestmentVariable(month_id)
        df_inv = inv.preprocess(demo=demo)
        self.db.append_to_db(df_inv, 'investments_variable')

        inf = InvestmentFixed(month_id)
        df_inf = inf.preprocess(demo=demo)
        self.db.append_to_db(df_inf, 'investments_fixed')

    @staticmethod
    def generate_dashboard():
        my_dashboard()

if __name__ == '__main__':

    # N.B. when using demo you must instantiate pipeline(), not just use e.g. pipeline.append_to_db()

    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--create", action=argparse.BooleanOptionalAction)  # create db tables
    parser.add_argument("--delete", action=argparse.BooleanOptionalAction)  # delete all db tables
    parser.add_argument("-a", "--append", action=argparse.BooleanOptionalAction)  # append to db
    parser.add_argument("-d", "--dashboard", action=argparse.BooleanOptionalAction)  # generate dashboard
    parser.add_argument("--demo", action=argparse.BooleanOptionalAction)  # use demo database

    parser.add_argument("-m", "--month")  # month
    parser.add_argument("-y", "--year")  # year

    args = parser.parse_args()

    if args.append and (args.month is None or args.year is None):
        parser.error("For 'append', both --month and --year must be specified.")

    if not (args.month is None or args.year is None):
        months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        if args.month[:3].upper() not in months:
            raise KeyError(f'"{args.month[:3].upper()}" is not a valid month.')

        month_id = f'{args.month[:3].upper()} {args.year[-2:]}'
        logging.info(f'month_id is "{month_id}"')

    if args.demo:
        os.environ['demo'] = 'True'

    pipe = pipeline()
    if args.delete:
        pipe.delete_all_db_tables()
    if args.create:
        pipe.create_tables_in_db()
    if args.append:
        pipe.append_to_db(month_id, args.demo)
    if args.dashboard:
        pipe.generate_dashboard()

