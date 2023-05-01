from sql import SQL
from api import *
import logging

sql = SQL()

mz = Monzo()
df_mz, months = mz.preprocess('statements/MonzoDataExport_January_2023-01-31_100153.csv')
sql.append_to_db(months, 'months')
sql.append_to_db(df_mz, 'spending_data')

bud = Budget()
df_bud = bud.preprocess()
sql.append_to_db(df_bud, 'budget')

acc = Accounts()
df_acc = acc.preprocess()
sql.append_to_db(df_acc, 'accounts')

inc = Income()
df_inc = inc.preprocess()
sql.append_to_db(df_inc, 'income')

inv = InvestmentVariable()
df_inv = inv.preprocess()
sql.append_to_db(df_inv, 'investments_variable')

inf = InvestmentFixed()
df_inf = inf.preprocess()
sql.append_to_db(df_inf, 'investments_fixed')