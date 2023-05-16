from dash_inputs import *
from palettes import vibrant, create_palette

import numpy as np
import calendar
from datetime import datetime

import plotly.graph_objects as go

from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
from dash.dash_table.Format import Format, Group, Scheme, Symbol


'''
MONTHLY COMPONENTS LAYOUT

----------------------------
|  1   |    4     |   7    |
----------------------------
|  2   |    5     |        |
------------------|   8    |
|  3   |    6     |        |
----------------------------

1 - title and month selection
2 - income summary
3 - investments summary
4 - bar chart of spending
5 - all spending data table
6 - spending timeline chart
7 - sunburst chart
8 - spending by subcategory table
'''

subcategory_palette = create_palette(vibrant)

money = Format(scheme=Scheme.fixed, precision=2,
               group=Group.yes, groups=3, group_delimiter=',',
               symbol=Symbol.yes, symbol_prefix=u'£')

# 1. TITLE AND MONTH SELECTION
def monthly_title():

    db = SQL()

    with db.engine.connect() as conn:
        query = text(f'SELECT * FROM months')

        df = pd.read_sql(sql=query,
                         con=conn)
    months = df.sort_values('Date', ascending=False).month_id.unique()

    return html.Div([
        html.H1(id='month_title',
                children=f'Monthly Spending\n {"month_id"}'),
        dcc.Dropdown(id='month_selection',
                     options=months,
                     placeholder="Select a month")
    ])

# 2. INCOME SUMMARY
def income_summary():
    return dcc.Graph(id='income', figure={})

def _update_income_summary(month_id):

    income = income_table(month_id)
    paycheck = income.set_index('Type').loc['Paycheck', 'Amount']

    taxes = income.Amount.sum() - paycheck
    taxes_str = ''
    for idx, row in income.iterrows():
        taxes_str += f'{row.Type}: £{abs(row.Amount):.2f},<br>'

    summary, _, monthly_spending = summary_table(month_id, total_row=False)
    bills = summary[summary.Subcategory == 'Bills'].Budget.sum()

    fig = go.Figure()

    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=paycheck,
            title={'text': 'paycheck', 'align': 'left', 'font': {'size': 20}},
            number={'prefix': '£', 'valueformat': ',.2f'},
            delta={'position': 'right', 'reference': 0, 'prefix': '£', 'valueformat': ',.2f'},
            domain={'x': [0.1, 0.9], 'y': [0.8, 0.9]}))

    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=abs(taxes),
            title={'text': 'taxes', 'align': 'left', 'font': {'size': 20}},
            number={'prefix': '£', 'valueformat': ',.2f'},
            delta={'position': 'right', 'reference': -(paycheck + 2 * taxes), 'prefix': '£', 'valueformat': ',.2f'},
            domain={'x': [0.1, 0.9], 'y': [0.55, 0.65]}))

    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=abs(bills),
            title={'text': 'bills', 'align': 'left', 'font': {'size': 20}},
            number={'prefix': '£', 'valueformat': ',.2f'},
            delta={'position': 'right', 'reference': -(paycheck + taxes + 2 * bills), 'prefix': '£',
                   'valueformat': ',.2f'},
            domain={'x': [0.1, 0.9], 'y': [0.3, 0.4]}))

    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=abs(monthly_spending),
            title={'text': 'spending', 'align': 'left', 'font': {'size': 20}},
            number={'prefix': '£', 'valueformat': ',.2f'},  # font
            delta={'position': 'right', 'reference': -(paycheck + taxes + bills + 2 * monthly_spending), 'prefix': '£',
                   'valueformat': ',.2f'},
            domain={'x': [0.1, 0.9], 'y': [0.05, 0.15]}))

    fig.add_annotation(x=0.49, y=0.6,
                       text='               ',
                       hovertext=taxes_str,
                       showarrow=False)

    fig.update_layout(height=300,
                      margin=dict(t=10, l=10, r=10, b=10)
                      )

    return fig

# 3. INVESTMENTS SUMMARY
def investments_summary():
    return dcc.Graph(id='investments', figure={})

def _update_investments_summary(month_id):

    dt_month_id_prev = datetime.strptime(month_id.lower(), '%b %y') - pd.DateOffset(months=1)
    month_id_prev = datetime.strftime(dt_month_id_prev, '%b %y').upper()

    _, liquidity = accounts_table(month_id)
    _, liquidity_prev = accounts_table(month_id_prev)

    inv_var, inv_fix, net_worth = investment_tables(month_id, liquidity)
    inv_var_prev, inv_fix_prev, net_worth_prev = investment_tables(month_id_prev, liquidity)

    fig = go.Figure()

    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=liquidity,
            title={'text': 'liquidity', 'align': 'left', 'font': {'size': 20}},
            number={'prefix': '£', 'valueformat': ',.2f'},
            delta={'position': 'right', 'reference': liquidity_prev, 'prefix': '£', 'valueformat': ',.2f'},
            domain={'x': [0.1, 0.9], 'y': [0.8, 0.9]}))

    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=inv_fix.Amount.sum(),
            title={'text': 'fixed investments', 'align': 'left', 'font': {'size': 20}},
            number={'prefix': '£', 'valueformat': ',.2f'},
            delta={'position': 'right', 'reference': inv_fix_prev.Amount.sum(), 'prefix': '£', 'valueformat': ',.2f'},
            domain={'x': [0.1, 0.9], 'y': [0.55, 0.65]}))

    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=inv_var.Value.sum(),
            title={'text': 'variable investments', 'align': 'left', 'font': {'size': 20}},
            number={'prefix': '£', 'valueformat': ',.2f'},
            delta={'position': 'right', 'reference': inv_var_prev.Value.sum(), 'prefix': '£', 'valueformat': ',.2f'},
            domain={'x': [0.1, 0.9], 'y': [0.3, 0.4]}))

    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=net_worth,
            title={'text': 'net worth', 'align': 'left', 'font': {'size': 20}},
            number={'prefix': '£', 'valueformat': ',.2f'},  # font
            delta={'position': 'right', 'reference': net_worth_prev, 'prefix': '£', 'valueformat': ',.2f'},
            domain={'x': [0.1, 0.9], 'y': [0.05, 0.15]}))

    fig.update_layout(height=300,
                      margin=dict(t=10, l=10, r=10, b=10), )

    return fig

# 4. BAR CHART OF SPENDING
def bar_chart():
    return html.Div([
        dbc.Row([
            dbc.Col([
                dcc.RadioItems(id='bar_plot_radio_item',
                               options=[{'label': 'Category ', 'value': 'Category'},
                                        {'label': 'Subategory ', 'value': 'Subcategory'}],
                               value='Subcategory',
                               inline=True),
                    ], width=6),
            dbc.Col([
                dcc.Checklist(id='bar_plot_checklist',
                              options=[{'label': 'Hide Empty', 'value': True}],
                              value=[True]),
                    ], width=6),
            ]),

            dcc.Graph(figure={},
                      id='bar_plot'),
            html.H6('This plot excludes the categories income and bills')
    ])

def _update_bar_chart(sub_category, hide_zeros, month_id):

    df, _, _ = summary_table(month_id, total_row=False)
    df = df[~df.Subcategory.isin(['Income', 'Bills'])]
    df.Total = -df.Total

    if hide_zeros:
        df = df[df.Total != 0]

    bar_fig = go.Figure(data=[
        go.Bar(name='Total',
               x=df[sub_category],
               y=df.Total,
               # TODO: how can I get it display subcategories in hovertemplate when grouped by category?
               # TODO: how can I add category total  in hovertemplate when grouped by category? Do this with a scatterplot?
               hovertemplate='%{x}<br>Spent: £%{y:.2f}',
               marker=dict(color=df["Subcategory"].apply(lambda x: subcategory_palette[x])),
               showlegend=False,
               # TODO: display diff to quantify how under/over budget we are (color coded if possible)
               # texttemplate='%{text:.2f}',
               # textposition='outside'
               ),
        go.Bar(name='Budget',
               x=df[sub_category],
               y=df.Budget,
               marker=dict(color='#D3D3D3'),
               hovertemplate='%{x}<br>Budget: £%{y:.2f}')
    ])

    bar_fig.update_layout(barmode='group',
                          legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
                          margin=dict(t=10, l=10, r=10, b=10),
                          height=350)

    bar_fig.update_yaxes(tickprefix='£')
    bar_fig.update_xaxes(tickangle=45)

    return bar_fig

# 5. ALL SPENDING TABLE
def all_spending_table():
    return dash_table.DataTable(
                id='all_spending_table',
                # data=spending.to_dict('records'),
                fixed_rows={'headers': True},
                style_table={'height': 210},
                # TODO: reduce row height
                # css=[{"selector": ".dash-spreadsheet tr th", "rule": "height: 20px;"},  # set height of header
                #      {"selector": ".dash-spreadsheet tr td", "rule": "height: 10px;"}],  # set height of body rows
                columns=[
                    {'id':'Date', 'name':'Date'},
                    {'id':'Subcategory', 'name':'Subcategory'},
                    {'id':'Name', 'name':'Name'},
                    {'id':'Description', 'name':'Description'},
                    {'id':'In', 'name':'In', 'type':'numeric', 'format':money},
                    {'id':'Out', 'name':'Out', 'type':'numeric', 'format':money},
                    {'id':'Balance', 'name':'Balance', 'type':'numeric', 'format':money}],
                sort_action='native',
                style_cell={'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                            'maxWidth': 0,
                            'font_size': '15px'},
                #tooltip_data=[{column: {'value': str(value), 'type': 'markdown'} for column, value in row.items() if column in ['Description', 'Name']}
                #              for row in spending.to_dict('records')],
                style_cell_conditional=[{'if': {'column_id': 'Date'}, 'width': '8%', 'textAlign':'left'},
                                        {'if': {'column_id': 'Subcategory'},'width': '15%', 'textAlign':'left'},
                                        {'if': {'column_id': 'Name'},'width': '16%', 'textAlign':'left'},
                                        {'if': {'column_id': 'Description'},'width': '26%', 'textAlign':'left'},
                                        {'if': {'column_id': 'In'},'width': '11%'},
                                        {'if': {'column_id': 'Out'},'width': '11%'},
                                        {'if': {'column_id': 'Balance'},'width': '13%'}],
                style_data_conditional=[{'if': {'filter_query': '{In} > 0', 'column_id':'In'}, 'color':'green'},
                                        {'if': {'filter_query': '{Out} > 0', 'column_id':'Out'}, 'color':'red'}],
            )

def _update_all_spending_table(month_id):
    spending = spending_table(month_id, dd_mm=True)
    return spending.to_dict('records')

# 6. SPENDING TIMELINE CHART
def timeline_chart():
    return html.Div([
        dcc.Graph(id='timeline_chart',
                  figure={}),
        html.H6('This plot excludes subcategories transfers and bills')
    ])

def _update_timeline_chart(month_id):

    df = spending_table(month_id, dd_mm=False)

    _, monthly_budget, _ = summary_table(month_id, total_row=True)

    start_d = df.Date[0]
    weekday, month_length = calendar.monthrange(start_d.year, start_d.month)

    dates = [datetime(start_d.year, start_d.month, day, 0, 0) for day in range(1, month_length + 1)]
    dates.append(datetime(start_d.year, start_d.month, month_length, 23, 59))

    daily_spend = monthly_budget / (len(dates) - 1)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(name='Spending',
                   x=df.Date,
                   y=df.Balance,
                   hovertemplate='£%{y:.2f}'))

    fig.add_trace(
        go.Scatter(name='Budget',
                   x=dates,
                   y=np.arange(monthly_budget, -daily_spend, -daily_spend),
                   line={'color': 'green', 'dash': 'dash'},
                   hovertemplate='£%{y:.2f}'
                   ))

    if df.Balance.iloc[-1] >= 0:
        fig.add_annotation(x=dates[-3], y=monthly_budget * 0.9,
                           text="Left to spend:",
                           showarrow=False)
        fig.add_annotation(x=dates[-3], y=monthly_budget * 0.8,
                           text=f'£{df.Balance.iloc[-1]:.2f}',
                           showarrow=False)
    else:
        fig.add_annotation(x=dates[-3], y=monthly_budget * 0.9,
                           text="Over budget",
                           showarrow=False)
        fig.add_annotation(x=dates[-3], y=monthly_budget * 0.8,
                           text=f'-£{-df.Balance.iloc[-1]:.2f}',
                           showarrow=False)

    fig.update_layout(hovermode='x',
                      showlegend=False,
                      margin=dict(t=10, l=10, r=10, b=10),
                      height=300)

    fig.update_yaxes(range=[df.Balance.min(), monthly_budget + 5],
                     tickmode='array',
                     tickvals=[0, monthly_budget],
                     showgrid=False,
                     tickprefix='£')

    fig.update_xaxes(tickmode='array',
                     tickvals=[dates[0], dates[-1]])

    return fig

# 7. SUNBURST CHART
def sunburst_chart():
    return html.Div([
        dcc.RadioItems(id='sunburst_radio_item',
                       options=[{'label':'Spending', 'value':'Total'},
                                {'label':'Budget', 'value':'Budget'}],
                       value='Total',
                       inline=True),
        dcc.Graph(id='sunburst',
                  figure={}),
        html.H6('This plot excludes the categories income and bills')
    ])

def _update_sunburst_chart(spend_budget, month_id):

    df, monthly_budget, monthly_spending = summary_table(month_id, total_row=False)
    df = df[~df.Subcategory.isin(['Income', 'Bills'])]
    df.Total = -df.Total

    labels = ['Total']
    parents = ['']
    if spend_budget == 'Total':
        values = [monthly_spending * -1]
    else:
        values = [monthly_budget]

    for cat in df.Category.unique():
        labels.append(cat + ' ')
        parents.append('Total')
        values.append(df[df.Category == cat][spend_budget].sum())

        if cat not in ['Holidays', 'Entertainment']:
            for subcat in df[df.Category == cat].Subcategory.unique():
                labels.append(subcat)
                parents.append(cat + ' ')
                values.append(df[df.Subcategory == subcat][spend_budget].sum())

    # Create sunburst plot
    sunburst = go.Figure(
        go.Sunburst(
            name='Sunburst',
            labels=labels,
            parents=parents,
            values=values,
            branchvalues='total',  # not column name Total
            # TODO can I add % of total spend to the hover? So it can double as a pie chart
            # hovertemplate='%{label}<br>£%{value:.2f} (%{value/values[0]}%)',
            hovertemplate='%{label}<br>£%{value:.2f}',
            marker=dict(colors=pd.Series(labels).apply(lambda x: subcategory_palette[x]))
        )
    )

    # Set plot title
    sunburst.update_layout(margin=dict(t=10, l=0, r=0, b=10),
                           height=350)

    return sunburst

# 8. SPENDING BY SUBCATEGORY TABLE
def spending_by_subcategory():
    return dash_table.DataTable(
        id='spending_by_subcategory_table',
        # data=df.drop('Category', axis=1).to_dict('records'),
        columns=[{'id':'Subcategory', 'name':'Subcategory'},
                 {'id':'In', 'name':'In', 'type':'numeric', 'format': money},
                 {'id':'Out', 'name':'Out', 'type':'numeric', 'format': money},
                 {'id':'Total', 'name':'Total', 'type':'numeric', 'format': money},
                 {'id':'Budget', 'name':'Budget', 'type':'numeric', 'format': money},
                 {'id':'Diff.', 'name':'Diff.', 'type':'numeric', 'format': money}],
        style_cell={'font_size': '15px'},
        style_cell_conditional=[{'if': {'column_id': 'Subcategory'}, 'width': '25%', 'textAlign':'left'},
                                {'if': {'column_id': 'In'},'width': '15%'},
                                {'if': {'column_id': 'Out'},'width': '15%'},
                                {'if': {'column_id': 'Total'},'width': '15%'},
                                {'if': {'column_id': 'Budget'},'width': '15%'},
                                {'if': {'column_id': 'Diff.'},'width': '15%'}],
        style_data_conditional=[{'if': {'filter_query': '{Diff.} > 0', 'column_id':'Diff.'}, 'color':'green'},
                                {'if': {'filter_query': '{Diff.} < 0', 'column_id':'Diff.'}, 'color':'red'},
                                # there must be a way to do this with a colour mapping
                                {'if': {'filter_query': '{Subcategory} = Income', 'column_id':'Subcategory'}, 'color':'green'},
                                #{'if': {'filter_query': '{Subcategory} = Transport', 'column_id':'Subcategory'}, 'color':subcategory_palette['Transport']},
                                #{'if': {'filter_query': '{Subcategory} = Car', 'column_id':'Subcategory'}, 'color':subcategory_palette['Car']},
                                #{'if': {'filter_query': '{Subcategory} = Groceries', 'column_id':'Subcategory'}, 'color':subcategory_palette['Groceries']},
                                #{'if': {'filter_query': '{Subcategory} = Snacks', 'column_id':'Subcategory'}, 'color':subcategory_palette['Snacks']},
                                #{'if': {'filter_query': '{Subcategory} = Lunch', 'column_id':'Subcategory'}, 'color':subcategory_palette['Lunch']},
                                #{'if': {'filter_query': '{Subcategory} = Eating out', 'column_id':'Subcategory'}, 'color':subcategory_palette['Eating out']},
                                #{'if': {'filter_query': '{Subcategory} = Alcohol', 'column_id':'Subcategory'}, 'color':subcategory_palette['Alcohol']},
                                #{'if': {'filter_query': '{Subcategory} = Shopping', 'column_id':'Subcategory'}, 'color':subcategory_palette['Shopping']},
                                #{'if': {'filter_query': '{Subcategory} = Clothes', 'column_id':'Subcategory'}, 'color':subcategory_palette['Clothes']},
                                #{'if': {'filter_query': '{Subcategory} = Electronics', 'column_id':'Subcategory'}, 'color':subcategory_palette['Electronics']},
                                #{'if': {'filter_query': '{Subcategory} = Personal care', 'column_id':'Subcategory'}, 'color':subcategory_palette['Personal care']},
                                #{'if': {'filter_query': '{Subcategory} = Gifts', 'column_id':'Subcategory'}, 'color':subcategory_palette['Gifts']},
                                #{'if': {'filter_query': '{Subcategory} = Entertainment', 'column_id':'Subcategory'}, 'color':subcategory_palette['Entertainment']},
                                #{'if': {'filter_query': '{Subcategory} = Holidays', 'column_id':'Subcategory'}, 'color':subcategory_palette['Holidays']},
                                #{'if': {'filter_query': '{Subcategory} = TOTAL', 'column_id':'Subcategory'}, 'color':'black'},
                                ],
        page_size=20)

def _update_spending_by_subcategory(month_id):
    df, _, _ = summary_table(month_id, total_row=False)
    return df.drop('Category', axis=1).to_dict('records')
