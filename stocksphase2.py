import psycopg2
import zipfile
import time
from itertools import combinations
from  builtins import any as b_any
from pymongo import MongoClient
import datetime

'''
@author-name:rishab katta
@author-name: milind kamath
@author-name: Bikash Roy
@author-name: Ankit Jain
'''

'''
Python Program to load stocks data into Postgresql server, change structure of the relational model,
determine funtional dependencies, load data from relational model to MongoDB for phase 2 of Project.

Dataset link: https://www.kaggle.com/ehallmar/daily-historical-stock-prices-1970-2018#historical_stocks.csv


NOTE: This program needs an empty Relational Database named "stocks" and that database needs to have user with 
SUPERUSER priviliges. You can do that by running the first query from any existing database on PGADMIN and then running
the 2nd and 3rd on newly created 'stocks' database

1: CREATE DATABASE stocks;
2: CREATE USER user101 WITH PASSWORD 'abcde' CREATEDB;
3: ALTER USER user101 WITH SUPERUSER;

 
'''

class DatabaseConnection:

    def __init__(self, host,port,pdb,pusername, ppwd):
        '''
        Constructor used for initializing MongoDB and Postgres database.
        :param host: hostname
        :param port: port number for mongodb
        :param pdb: postgres database name
        :param pusername: postgres username for above database with SUPERUSER priviliges
        :param ppwd: postgres user password
        '''
        try:
            self.client = MongoClient(host, port)
            self.database = self.client['stocks']
            self.connection = psycopg2.connect(host=str(host), database=str(pdb), user=str(pusername), password=str(ppwd))
            # self.connection = psycopg2.connect(host="localhost", database="stocks", user="user101", password="abcde")
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            print(getattr(e, 'message', str(e)))

    def create_tables(self):
        '''
        create tables for relational database
        :return: None
        '''

        self.cursor.execute("CREATE TABLE company(ticker VARCHAR NOT NULL, exchange VARCHAR, "
                            "company_name VARCHAR, sector VARCHAR, industry VARCHAR, PRIMARY KEY(ticker))")

        self.cursor.execute("Create table historical_stock_price(id BIGSERIAL PRIMARY KEY, ticker VARCHAR, open_price float, "
                            "close_price float, adj_close_price float, low_price float, high_price float, volume BIGINT, "
                            "stock_date DATE, FOREIGN KEY(ticker) REFERENCES Company(ticker))")

    def insert_tables(self, path):
        '''
        Insert data from downloaded dataset to the relational database
        :param path: Pathname for the downloaded zip file
        :return: None
        '''

        with zipfile.ZipFile(str(path) +"daily-historical-stock-prices-1970-2018.zip", "r") as zip_ref:
            zip_ref.extractall(str(path))


        file=str(path) + "daily-historical-stock-prices-1970-2018/" + "historical_stocks.csv"
        self.cursor.execute("COPY company(ticker, exchange, company_name, sector,"
                          " industry) FROM %s DELIMITER ',' CSV HEADER", (file,))



        file = str(path) + "daily-historical-stock-prices-1970-2018/" + "historical_stock_prices.csv"
        self.cursor.execute("COPY historical_stock_price(ticker, open_price, close_price, adj_close_price,"
                            " low_price, high_price, volume, stock_date) FROM %s DELIMITER ',' CSV HEADER", (file,))
    def change_structure(self):
        '''
        Change structure based on Instructions from Prof. Mior
        :return: None
        '''

        self.cursor.execute("create table sector(id bigserial, name VARCHAR)")

        self.cursor.execute("insert into sector(name) select distinct sector from company ")

        self.cursor.execute("ALTER TABLE company ADD COLUMN sectorid bigint ")

        self.cursor.execute("UPDATE company SET sectorid = sector.id FROM sector WHERE sector.name = company.sector")

        self.cursor.execute("create table industry(id bigserial, name VARCHAR)")

        self.cursor.execute("insert into industry(name) select distinct industry from company")

        self.cursor.execute("ALTER TABLE company ADD COLUMN industryid bigint ")

        self.cursor.execute("UPDATE company SET industryid = industry.id FROM industry WHERE industry.name = company.industry")

        self.cursor.execute("ALTER TABLE company DROP COLUMN sector, DROP COLUMN industry")

        self.cursor.execute("ALTER TABLE company RENAME COLUMN sectorid TO sector")

        self.cursor.execute("ALTER TABLE company RENAME COLUMN industryid TO industry")

        self.cursor.execute("ALTER TABLE sector ADD CONSTRAINT pk_sector PRIMARY KEY (id)")

        self.cursor.execute("ALTER TABLE industry ADD CONSTRAINT pk_industry PRIMARY KEY (id)")

        self.cursor.execute("ALTER TABLE company ADD CONSTRAINT fk_sector FOREIGN KEY (sector) REFERENCES sector (id)")

        self.cursor.execute("ALTER TABLE company ADD CONSTRAINT fk_industry FOREIGN KEY (industry) REFERENCES industry (id)")

    def func_depd_pruning(self):
        '''
        function for determining functional dependencies using the pruning approach. This function determines functional dps
        for company as a sample. We have run the same code for other tables in our model and the results are documented in the write-up
        :return: None
        '''
        input = ['ticker', 'exchange', 'company_name', 'sector', 'industry']

        output = sum([list(map(list, combinations(input, i))) for i in range(3)], [])

        output.pop(0)  # deleting the empty set

        concat_list = [', '.join(sub_list) for sub_list in output]

        table_list = []
        table_dict = {}

        for column in concat_list:
            column_list = []
            query = "SELECT array_agg(ticker) FROM company GROUP BY " + column + " order by " + column
            self.cursor.execute(query)
            arrays = self.cursor.fetchall()
            for array in arrays:
                array = str(array)
                array = array.strip('()[],')
                column_list.append(array.translate('()[]').split(', '))
            table_list.append(column_list)

        table_dict.clear()

        for i in range(0, len(concat_list)):
            table_dict[concat_list[i]] = table_list[i]

        func_depd = []

        for left_col in table_dict.keys():
            for right_col in input:
                count = 0
                lolleft = table_dict[left_col]
                lolright = table_dict[right_col]
                for left_list in lolleft:
                    for right_list in lolright:
                        if set(left_list) <= set(right_list):
                            count += 1
                            break
                        else:
                            continue
                if count == len(lolleft):
                    leftcollist = left_col.split(", ")
                    someflag = True
                    for col in leftcollist:
                        if col.strip(" ") != right_col.strip(" "):
                            continue
                        else:
                            someflag = False
                    if someflag:
                        word = "-->" + str(right_col)
                        if not b_any(word in x for x in func_depd):
                            func_depd.append(left_col + "-->" + right_col)

        print(func_depd)

    def insert_mongodb(self):
        '''
        Insert data from relational database to MongoDB
        :return: None
        '''

        self.cursor.execute("select id, ticker, open_price, close_price, adj_close_price, low_price, high_price, volume, stock_date from historical_stock_price ")
        historical_stock_prices = self.cursor.fetchall()
        self.collection = self.database['historical_stock_price']

        for hsp in historical_stock_prices:
            self.hspdoc = {}
            self.hspdoc['_id'] = hsp[0]
            self.hspdoc['ticker'] = hsp[1]
            if hsp[2] is not None:
                self.hspdoc['open_price'] = hsp[2]
            if hsp[3] is not None:
                self.hspdoc['close_price'] = hsp[3]
            if hsp[4] is not None:
                self.hspdoc['adj_close_price'] = hsp[4]
            if hsp[5] is not None:
                self.hspdoc['low_price'] = hsp[5]
            if hsp[6] is not None:
                self.hspdoc['high_price'] = hsp[6]
            if hsp[7] is not None:
                self.hspdoc['volume'] = hsp[7]
            if hsp[8] is not None:
                self.hspdoc['stock_date'] = datetime.datetime.combine(hsp[8], datetime.time.min)



            self.collection.insert_one(self.hspdoc)

#############################################################################################

        self.cursor.execute("select id, name from sector ")
        sectors = self.cursor.fetchall()
        self.collection = self.database['sector']

        for sector in sectors:
            self.sectordoc = {}
            self.sectordoc['_id'] = sector[0]
            self.sectordoc['name'] = sector[1]

            self.collection.insert_one(self.sectordoc)

#############################################################################################

        self.cursor.execute("select id, name from industry ")
        industries = self.cursor.fetchall()
        self.collection = self.database['industry']

        for industry in industries:
            self.industrydoc = {}
            self.industrydoc['_id'] = industry[0]
            self.industrydoc['name'] = industry[1]

            self.collection.insert_one(self.industrydoc)

#############################################################################################

        self.cursor.execute("select ticker, exchange, company_name, sector, industry from company ")
        companies = self.cursor.fetchall()
        self.collection = self.database['company']

        for company in companies:
            self.companydoc = {}
            self.companydoc['ticker'] = company[0]
            self.companydoc['exchange'] = company[1]
            if company[2] is not None:
                self.companydoc['company_name'] = company[2]
            if company[3] is not None:
                self.companydoc['sector'] = company[3]
            if company[4] is not None:
                self.companydoc['industry'] = company[4]


            self.collection.insert_one(self.companydoc)


    def runquery(self):
        '''
        execute queries for the questions we came up with on the stocks database
        :return: None
        '''
        print("Executing queries without index")
        print("Query 1")
        start = time.time()
        self.cursor.execute("select distinct company_name, max((((close_price - "
                            "adj_close_price) / close_price) * 100)) as MaxPercentageChange "
                            "from company "
                            "join historical_stock_price on  historical_stock_price.ticker = company.ticker "
                            "where cast(stock_date as varchar) > '2000-01-01' "
                            "and cast(stock_date as varchar) < '2018-12-31' "
                            "and (((close_price - adj_close_price) / close_price) * 100) >= 15 "
                            "group by company_name order by MaxPercentageChange")

        end = time.time()
        print("Time taken for query1", end-start, "seconds")

        print("Query 2")
        start = time.time()
        self.cursor.execute("select distinct company_name, avg(open_price) as AvgOpenPrice from company "
                            "join historical_stock_price on  historical_stock_price.ticker = company.ticker "
                            "where cast(stock_date as varchar) > '1980-01-01' "
                            "and cast(stock_date as varchar) < '2018-12-31' "
                            "and company_name ilike '%Limited' or company_name ilike '%inc' group by company_name "
                            "having avg(open_price) > 30")

        end = time.time()
        print("Time taken for query2", end - start, "seconds")

        print("Query 3")
        start = time.time()
        self.cursor.execute("select company_name, sec.name, hist.high_price - hist.low_price "
                            "as diffInPrediction from company as comp join sector as sec on comp.sector = sec.id "
                            "join historical_stock_price as hist on comp.ticker = hist.ticker "
                            "where sec.name ilike 'Technology' and hist.high_price - hist.low_price < 0.02 "
                            "order by company_name")

        end = time.time()
        print("Time taken for query3", end - start, "seconds")

        print("Query 4")
        start = time.time()
        self.cursor.execute("select company_name, volume from company as comp join sector as sec "
                            "on comp.sector = sec.id "
                            "join historical_stock_price as hist on comp.ticker = hist.ticker "
                            "where comp.exchange ilike 'NASDAQ' and sec.name ilike 'Health Care' "
                            "and (((close_price - adj_close_price) / close_price) * 100) >= 50 "
                            "order by volume desc")

        end = time.time()
        print("Time taken for query4", end - start, "seconds")

        print("Query 5")
        start = time.time()
        self.cursor.execute("select distinct comp.company_name, Max(hist.close_price - hist.open_price) as MaxLoss , "
                            "((Max(hist.close_price - hist.open_price))*hist.volume) as MaxAmountLoss  "
                            "from industry as ind "
                            "join company as comp on ind.id = comp.industry "
                            "join historical_stock_price as hist on hist.ticker = comp.ticker "
                            "where cast(hist.stock_date as varchar) > '2015-01-01' "
                            "and cast(hist.stock_date as varchar) < '2018-01-01' "
                            "and ind.name ilike 'Integrated Oil Companies' "
                            "group by comp.company_name, hist.volume order by MaxLoss desc")

        end = time.time()
        print("Time taken for query5", end - start, "seconds")

        ###################################################################################################

    def createindex(self):
        '''
        Create indexes on columns of tables in the stocks database to speed up query execution.
        :return: None
        '''
        print("Creating indexes")

        self.cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

        self.cursor.execute("CREATE INDEX ticker_idx ON company (ticker);")
        self.cursor.execute("CREATE INDEX ticker_hist_idx ON historical_stock_price (ticker);")
        self.cursor.execute("CREATE INDEX company_name_idx ON company USING GIN (company_name gin_trgm_ops)")
        self.cursor.execute("CREATE INDEX sector_name_idx ON sector USING GIN (name gin_trgm_ops)")
        self.cursor.execute("CREATE INDEX company_exchange_idx ON company USING GIN (exchange gin_trgm_ops)")
        self.cursor.execute("CREATE INDEX industry_name_idx ON industry USING GIN (name gin_trgm_ops)")

        print("Indexes created")



    def dropindex(self):
        '''
        Drop indexes if they already exist
        :return:
        '''

        self.cursor.execute("DROP INDEX IF EXISTS ticker_idx, ticker_hist_idx, company_name_idx, sector_name_idx, "
                            "company_exchange_idx, industry_name_idx")

    def droptables(self):
        '''
        Drop tables from stocks database if they exist
        :return:
        '''
        self.cursor.execute("DROP TABLE IF EXISTS company, historical_stock_price, sector, industry CASCADE")

if __name__ == '__main__':
    port = int(input("Enter port MongoDB's running on"))
    host = input("Enter host for both MongoDB and Postgres")
    pdb = input("Enter Postgres Database Name")
    pun = input("Enter postgres username")
    ppwd = input("Enter postgres password")
    path = str(input("Enter Path except the file name - example- C:/users/files/"))

    database_connection = DatabaseConnection(host,port, pdb, pun, ppwd)
    database_connection.droptables()
    database_connection.create_tables()
    database_connection.insert_tables(path)
    database_connection.change_structure()
    database_connection.func_depd_pruning()
    database_connection.insert_mongodb()
    database_connection.dropindex()
    database_connection.runquery()
    database_connection.createindex()
    database_connection.runquery()