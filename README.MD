# Welcome to my Finances API and Dashboard!

![dashboard](dashboard%20example.JPG)

## 1. Introduction

This is the API and dashboard that I use every month to keep track of my finances and spending. It inputs data from my 
monzo bank statements to a sqlite database and represents useful summary statistics and plots in an interactive dashboard.

I have used the python packages plotly and dash for the front end, allowing you to easily switch between months and 
analyse monthly trends with customisable plots.

## 2. Setup
- clone this repository using ```git clone https://github.com/erwallace/Finances-API.git```.
- open the cloned directory on your local drive and run ```Finances-API\install_api.bat```.

You are now ready to go!

## 3. Demo

Demon data is provided to demonstrate the required format of input data, however this data can also be used to create a 
demo dashboard. This is done using ```src\setup_demo.bat```. This batch file will delete any previous demo database, 
create a new database, populate it with all the demo data and generate a dashboard. 

This allows you to easily interact with the dashboard and see all the features without requiring any of your own data!

## 4. Input Files
For each month you will require 3 input files: 
- a monzo csv bank statement (MonzoDataExport_{month}_{year}.csv).
- inputs_{mm)_{yy}.csv : this contains any monthly budgeting, end of the month balances of any accounts and 
paycheck/taxes information.
- investments_variable_{mm)_{yy}.csv : this contains the unit price and number of units owned 
of any stocks owned.

Examples of each of these files are available in Finances-API/src/data/.

## 5. Running

Remember to activate the virtual environment before running the script if you're not using an IDE:

```call ..\venv\Scripts\activate```

The API is run via the command line from the location of the pipe.py script (i.e. /Finances-API/src) with the following 
notation:

```python pipe.py [action(s)]```

**actions**
- *--create* / *-c* : creates new tables in the database (previous tables need to have been removed prior)
- *--delete* : deletes all tables and their contents from the specified database
- *--append* / *-a* : appends data from the date given to the database. must also specify a date (see below)
- *--dashboard* / *-d* : generates a link for the dashboard. A date must still be specified for this action
- *--demo* : can be used to select demo data/database/dashboard (see setup_demo.bat)

**date**
- *--month* / *-m* : specifies the month to be appended (July/july/jul/JUL would all be accepted for July).
- *--year* / *-y* : specifies the year to be appended (YY or YYYY)

**e.g.** ```python pipe.py --month July --year 2023 --append --dashboard``` is equivalent to 
```python pipe.py -m jul -y 23 -a -d``` and will append data from July 2023 to the database before giving a link to the 
dashboard.

## 6. Unit Testing 

All unit tests should be run from the root directory (/Finances-API) using the command ```python -m pytest```. This will 
run all unit tests for the API. To run specific test modules, classes or units using 
```python -m pytest  <path to test module>::<test class name>::<unit test name>```.

### Thanks for reading! I hope you find this as useful as I have!

