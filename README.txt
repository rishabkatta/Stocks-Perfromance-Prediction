First run query1 provided in the db_init_text file in pgadmin from an existing database - CREATE DATABASE stocks;
Then go to "stocks" database in pgadmin and run query 2 to create a user to that database - CREATE USER user101 WITH PASSWORD 'abcde' CREATEDB;
After the user is created run query 3 to alter user priviliges to use COPY statements(Must be a SUPERUSER) - ALTER USER user101 WITH SUPERUSER; 

The link to download to datasets is provided in the link_to_dataset text file.

https://www.kaggle.com/ehallmar/daily-historical-stock-prices-1970-2018#historical_stocks.csv