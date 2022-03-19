import pyodbc
import pandas as pd
from preprocess_data import DataPipeLine
from queries import *

class DataAccessLayer:
    def __init__(self, con_driver, server_name, db_name, uid, pwd):
        try:
            temp_conn = pyodbc.connect("DRIVER={0}; \
                                  SERVER={1}; \
                                  DATABASE={2}; \
                                  UID={3}; \
                                  PWD={4}; \
                                  Encrypt=yes;TrustServerCertificate=yes;".format(con_driver, server_name, db_name, uid, pwd),
                                  autocommit=True)
            print('connect {0} success'.format(db_name))
            self.conn = temp_conn
        except:
            print('{0} does not exist, try to create...'.format(db_name))
            masterdb_conn = pyodbc.connect("DRIVER={0}; \
                                  SERVER={1}; \
                                  DATABASE={2}; \
                                  UID={3}; \
                                  PWD={4}; \
                                  Encrypt=yes;TrustServerCertificate=yes;".format(con_driver, server_name, 'master', uid, pwd),
                                  autocommit=True)
            masterdb_cursor = masterdb_conn.cursor()
            masterdb_cursor.execute("CREATE DATABASE {0}".format(db_name))
            masterdb_cursor.close()
            masterdb_conn.close()
            try:
                db_conn = pyodbc.connect("DRIVER={0}; \
                                  SERVER={1}; \
                                  DATABASE={2}; \
                                  UID={3}; \
                                  PWD={4}; \
                                  Encrypt=yes;TrustServerCertificate=yes;".format(con_driver, server_name, db_name, uid, pwd),
                                  autocommit=True)
                print('create and connect {0} success'.format(db_name))
                self.conn = db_conn
            except:
                print('failed! re-check your program')

    def init_tables(self, li_queries):
        cur = self.conn.cursor()
        for query in li_queries:
            try:
                cur.execute(query)
                print('init table success')
            except:
                print('init table failed')
        cur.close()
    
    
    def insert_by_dataframe(self, cur, insert_query, data, table_name):
        try:
            for idx, row in data.iterrows():
                cur.execute(insert_query, list(row))
            
            print(f"insert {table_name} success")
        except:
            print(f"insert {table_name} failed")
        

    def insert_data(self, data_path):
        data_pip = DataPipeLine.preprocess_data(data_path)
        df = data_pip.data
        cur = self.conn.cursor()

        data_dim_cus = df[['User_ID', 'Year_Birth', 'Education', 'Marital_Status', 'Income', 'Kidhome', 'Teenhome', 'Country']].copy(deep=True)
        self.insert_by_dataframe(cur, insert_dim_customer, data_dim_cus, "dim_customer")

        data_dim_date = pd.to_datetime(df['Date_Enroll'].copy(deep=True))
        data_dim_date.drop_duplicates(inplace=True)

        try:
            for idx, value in data_dim_date.items():
                cur.execute(insert_dim_date, [value, value.year, value.month, value.day])
            print("insert dim_date success")
        except:
            print("insert dim_date failed")

        data_fact_ma = df[['User_ID', 'Date_Enroll', 'NumDealsPurchases', 'NumWebPurchases', 'NumCatalogPurchases', 'NumStorePurchases' 
                        , 'NumWebVisitsMonth', 'MntWines', 'MntFruits', 'MntFishs', 'MntSweets', 'MntGolds', 'MntMeats', 'Response', 'Complain'
                        , 'Recency']].copy(deep=True)
        
        data_fact_ma["total_campaigns_accepted"] = df["AcceptedCmp1"] + \
                                                df["AcceptedCmp2"] + \
                                                df["AcceptedCmp3"] + \
                                                df["AcceptedCmp4"] + \
                                                df["AcceptedCmp5"]

        data_fact_ma["total_spent"] = data_fact_ma["MntWines"] + \
                                    data_fact_ma["MntFruits"] + \
                                    data_fact_ma["MntMeats"] + \
                                    data_fact_ma["MntFishs"] + \
                                    data_fact_ma["MntSweets"] + \
                                    data_fact_ma["MntGolds"]
        self.insert_by_dataframe(cur, insert_fact, data_fact_ma, "fact_ma")

if __name__ == '__main__':
    con_db = DataAccessLayer('ODBC Driver 18 for SQL Server',
                            'localhost',
                            'DataWarehouse_MarketingAnalytics',
                            'sa',
                            'xxxxxxxxxxx')
    con_db.init_tables(init_tables_query)
    con_db.insert_data("marketing_data.csv")
    