from sql import SQL
from api import *
import logging

sql = SQL()

mz = Monzo()
df_mz, months = mz.preprocess('statements/MonzoDataExport_March_2023-04-01_085048.csv')
sql.append_to_db(months, 'months')
sql.append_to_db(df_mz, 'spending_data')

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