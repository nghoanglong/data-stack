import pyodbc

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
                                  Encrypt=yes;TrustServerCertificate=yes;".format(con_driver, server_name, 'master', uid, pwd),
                                  autocommit=True)
                print('create and connect {0} success'.format(db_name))
                self.conn = db_conn
            except:
                print('failed! re-check your program')


if __name__ == '__main__':
    con_db = DataAccessLayer('ODBC Driver 18 for SQL Server',
                            'localhost',
                            'DataWarehouse_MarketingAnalytics',
                            'sa',
                            'nghoanglong1712')
        