call ..\venv\Scripts\activate

python pipe.py --month may --year 2023 --delete --demo

python pipe.py --month may --year 2023 --create --append --demo
python pipe.py --month june --year 2023 --append --demo
python pipe.py --month july --year 2023 --append --dashboard --demo

pause