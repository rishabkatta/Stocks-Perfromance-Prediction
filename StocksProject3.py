'''
@author-name: Rishab Katta
@author-name: Milind Kamath
@author-name: Bikash Roy
@author-name: Ankit Jain

Python program for performing Data Cleaning, Data Integration and Itemset Mining on the stocks database.

NOTE: This code assumes that there's already stocks relational database with modified structure(modified in phase 2)
'''

import psycopg2
import time
import itertools


class DatabaseConnection:

    def __init__(self,h,db,username,pwd):
        '''
        Constructor is used to connect to the database
        :param h: hostname
        :param db: database name
        :param username: Username
        :param pwd: password
        '''
        try:
            self.connection = psycopg2.connect(host=str(h), database=str(db), user=str(username), password=str(pwd))
            # self.connection = psycopg2.connect(host="localhost", database="stocks", user="user2", password="abcde")
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            print(getattr(e, 'message', str(e)))

    def cleaning_data(self):
        # Deleting rows from historical_stock_price where open_price or close price is null because we're going to be performing calculations on them when we mine.
        self.cursor.execute("Delete from historical_stock_price where open_price is null") # 0 rows deleted

        self.cursor.execute("Delete from historical_stock_price where close_price is null") # 0 rows deleted

        #not null constraint for future addition of rows
        self.cursor.execute("ALTER TABLE historical_stock_price ALTER COLUMN open_price SET NOT NULL, ALTER COLUMN close_price SET NOT NULL")

        # There's a foreign key condition on ticker column of historical_stock_price to ticker of Company where ticker is Primary Key in Company.
        self.cursor.execute("Delete from historical_stock_price where ticker is null or ticker ilike 'n/a'") # 0 rows deleted

        #Checking if all rows have the right date format.
        self.cursor.execute("select ( SELECT count(to_char(stock_date,'%Y-%m-%d')) AS niceDate FROM historical_stock_price "
                            ") - (SELECT count(*) from historical_stock_price) as total_count")
        rows = self.cursor.fetchone()
        if rows[0] == 0:
            print("All Dates are in right format")

    def integrating_data(self):

        # LAV finance_companies on company table
        self.cursor.execute("create materialized view finance_companies as "
                            "select ticker, exchange, company_name, s.name as sector, i.name as industry from company c "
                            "inner join sector s on s.id = c.sector inner join industry i on i.id = c.industry "
                            "where s.name ilike 'FINANCE'")

        # LAV tech_companies on company table
        self.cursor.execute("create materialized view tech_companies as "
                            "select ticker, exchange, company_name, s.name as sector, i.name as industry from company c "
                            "inner join sector s on s.id = c.sector inner join industry i on i.id = c.industry "
                            "where s.name ilike 'TECHNOLOGY'")

        #GAV on finance_companies and tech_companies
        self.cursor.execute("create materialized view fin_tech_companies as "
                            "select * from finance_companies "
                            "union "
                            "select * from tech_companies")


    def popular_fintech_companies(self):
        '''
        Create and load popular_fintech_companies table which is a subset of historical_stock_price table containing only
        finance and tech companies and whose closing price is increased by more than 20% in the same day.
        :return: None
        '''

        self.cursor.execute("create table popular_fintech_companies as "
                            "select id, hsp.ticker, open_price, close_price, stock_date from historical_stock_price hsp "
                            "inner join fin_tech_companies fa on hsp.ticker= fa.ticker where close_price > open_price*1.2")
        print("Total number of rows inserted into PFC: " + str(self.cursor.rowcount))

    def l1(self):
        '''
        Create First Level of the Lattice with ticker and the count of days when the closing price increased by more than 20% than the opening price in the same day
        :return: None
        '''

        self.cursor.execute("create table l1 as "
                            "select pfc.ticker as ticker1, count(*) from popular_fintech_companies pfc group by pfc.ticker having count(*) >= 5 ")

        print("Total number of rows inserted into l1: " + str(self.cursor.rowcount))


    def generalize(self):
        '''
        Generalized code for generating all levels of lattice
        :return: None
        '''
        print(" ")
        print("Executing Generalized code...")

        self.cursor.execute("create table l1 as "
                            "select pfc.ticker as ticker1, count(*) from popular_fintech_companies pfc group by pfc.ticker having count(*) >= 5 ")
        print("Total number of rows inserted into l1: " + str(self.cursor.rowcount))

        rows =self.cursor.rowcount
        k=2


        while rows!=0:

            query = "create table l" + str(k) + " as " \
                    "select " + self.q_p1(k) + " count(*) as count from " + self.q_p2(k) + " where " + self.q_p3(k)+ self.q_p4(k) + " group by " + self.q_p5(k) + " having count(*) >= 5"

            self.cursor.execute(query)
            rows = self.cursor.rowcount
            print("Total number of rows inserted into l" + str(k) +": "+ str(self.cursor.rowcount))
            k +=1

        self.cursor.execute("select ticker1, t1.company_name, ticker2, t2.company_name, ticker3, t3.company_name "
                            "from l3 left join company t1 on ticker1=t1.ticker join company t2 on ticker2=t2.ticker join company t3 on ticker3=t3.ticker")

        rows = self.cursor.fetchall()

        print("Final Level with non-empty rows is L3. The Names of Companies in that level are")
        for row in rows:
            print(row)


    def q_p1(self,k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''
        q_string1=""

        for i in range(1,k+1):
            q_string1 += "pfc"+str(i)+".ticker as ticker"+str(i) +", "

        return q_string1

    def q_p2(self, k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''

        q_string2=""

        for i in range(1,k+1):
            if i==k:
                q_string2 += "popular_fintech_companies pfc" + str(i)
            else:
                q_string2 += "popular_fintech_companies pfc" + str(i) + " cross join "
        return q_string2

    def q_p3(self, k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''
        q_string3=""

        for i in range(1,k):
            q_string3 += "pfc" +str(i) +".stock_date = pfc"+str(i+1) +".stock_date and "

        return q_string3

    def q_p4(self, k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''
        q_string4=""

        for i in range(1,k):
            for j in range (i+1,k+1):
                q_string4 += "pfc" + str(i) + ".ticker < pfc" + str(j) + ".ticker and "
        q_string4=q_string4.rstrip("and ")

        return q_string4

    def q_p5(self, k):
        '''
        Helper for generalize() function
        :param k: level number of the lattice
        :return: string
        '''

        q_string5=""
        for i in range(1,k+1):
            if i==k:
                q_string5 += "pfc" + str(i) + ".ticker "
            else:
                q_string5 += "pfc" + str(i) + ".ticker, "
        return q_string5

    def association_rules(self):
        '''
        Discover Association rules for the Maximum frequent itemset.
        :return: None
        '''
        self.cursor.execute("select ticker1, ticker2, ticker3 from L3")
        rows = self.cursor.fetchall()
        confidence = 50

        for row in rows:
            for L in range(0, len(row) + 1):
                for subset in itertools.permutations(row, L):
                    if len(subset) > 1:
                        if len(subset)<3:
                            self.cursor.execute("select count from l" + str(len(subset))+ " where ticker1 ilike \'" + str(subset[0]) +"\' and ticker2 ilike \'" + str(subset[1])+ "\'")
                            numer = self.cursor.fetchone()
                            self.cursor.execute("select count from l" + str(len(subset)-1) + " where ticker1 ilike \'" + str(subset[0]) + "\'")
                            denom = self.cursor.fetchone()
                            if numer is not None and denom is not None:
                                if (numer[0]/denom[0]) * 100 > confidence:
                                        print(subset[0], "->", subset[1])
                        else:
                            col1= str(subset[0])
                            col2= str(subset[1])
                            col3 = str(subset[2])
                            self.cursor.execute("select count from l" + str(len(subset))+ " where ticker1 ilike \'" + col1 +"\' and ticker2 ilike \'" + col2 + "\' and ticker3 ilike \'" + col3 + "\'")
                            numer = self.cursor.fetchone()
                            self.cursor.execute("select count from l" + str(len(subset) - 2) + " where ticker1 ilike \'" + col1 + "\'")
                            denom = self.cursor.fetchone()
                            if numer is not None and denom is not None:
                                if (numer[0]/denom[0]) * 100 > confidence:
                                    print(subset[0], "->", subset[1] + "," + subset[2])

                            self.cursor.execute("select count from l" + str(len(subset) - 1) + " where ticker1 ilike \'" + col1 + "\'" + " and ticker2 ilike \'" + col2 + "\'")
                            denom = self.cursor.fetchone()
                            if numer is not None and denom is not None:
                                if (numer[0] / denom[0]) * 100 > confidence:
                                    print(subset[0] + "," + subset[1] + "->" + subset[2])

    def drop_tables_with_pfc(self):
        '''
        Drop all tables if they exist including popular_fintech_companies
        :return: None
        '''
        self.cursor.execute("DROP TABLE IF EXISTS popular_fintech_companies, l1, l2, l3,l4 CASCADE")
    def drop_tables_wo_pfc(self):
        '''
        Drop all tables if they exist excluding popular_fintech_companies
        :return: None
        '''
        self.cursor.execute("DROP TABLE IF EXISTS l1, l2, l3,l4  CASCADE")


if __name__ == '__main__':
    h = str(input("Enter host name"))
    db = str(input("Enter Database Name"))
    username = str(input("Enter username"))
    pwd = str(input("Enter password"))
    db_con =DatabaseConnection(h,db,username,pwd)

    db_con.drop_tables_with_pfc()

    start_time = time.time()
    db_con.cleaning_data()
    print("--- %s seconds for cleaning data ---" % (time.time() - start_time))
    #
    start_time = time.time()
    db_con.integrating_data()
    print("--- %s seconds for integrating data ---" % (time.time() - start_time))

    db_con.drop_tables_with_pfc()
    #
    start_time = time.time()
    db_con.popular_fintech_companies()
    print("--- %s seconds for pfc ---" % (time.time() - start_time))
    db_con.drop_tables_wo_pfc()
    #
    # start_time = time.time()
    # db_con.l1()
    # print("--- %s seconds for l1 ---" % (time.time() - start_time))

    start_time = time.time()
    db_con.generalize()
    print("--- %s seconds for generalized code ---" % (time.time() - start_time))

    start_time = time.time()
    print("Association Rules with more than 50 percent confidence")
    db_con.association_rules()
    print("--- %s seconds for generating association rules ---" % (time.time() - start_time))



