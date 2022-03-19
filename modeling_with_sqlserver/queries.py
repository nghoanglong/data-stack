# CREATE TABLES
init_fact_tables = """CREATE TABLE FACT(
                    user_id INT NOT NULL,
                    date_id DATE NOT NULL,
                    numdealspurchases INT,
                    numwebpurchases INT,
                    numcatalogpurchases INT,
                    numstorepurchases INT,
                    numwebvisitmonth INT,
                    mntwines FLOAT,
                    mntfruits FLOAT,
                    mntfishs FLOAT,
                    mntsweets FLOAT,
                    mntgolds FLOAT,
                    mntmeats FLOAT,
                    response INT,
                    complain INT,
                    recency INT,
                    total_campaigns_accepted INT,
                    total_spent FLOAT,
                    CONSTRAINT PK_FACT PRIMARY KEY(user_id, date_id)
)"""

init_dim_date = """CREATE TABLE DIM_DATE(
                date_id DATE NOT NULL,
                year_enroll INT,
                month_enroll INT,
                day_enroll INT,
                CONSTRAINT PK_DIM_DATE PRIMARY KEY(date_id)
)"""

init_dim_customer = """CREATE TABLE DIM_CUSTOMER(
                    user_id INT NOT NULL,
                    year_birth INT,
                    education VARCHAR(255),
                    marital_status VARCHAR(255),
                    income INT,
                    kidhome INT,
                    teenhome INT,
                    country VARCHAR(255),
                    CONSTRAINT PK_DIM_CUS PRIMARY KEY(user_id)
)"""

insert_dim_customer = """INSERT INTO DIM_CUSTOMER(user_id, 
                                                year_birth, 
                                                education, 
                                                marital_status, 
                                                income, 
                                                kidhome, 
                                                teenhome, 
                                                country) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"""

insert_dim_date = """INSERT INTO DIM_DATE(date_id, 
                                        year_enroll, 
                                        month_enroll,
                                        day_enroll) VALUES(?, ?, ?, ?)"""

insert_fact = """INSERT INTO FACT(user_id, 
                                date_id, 
                                numdealspurchases,
                                numwebpurchases,
                                numcatalogpurchases,
                                numstorepurchases,
                                numwebvisitmonth,
                                mntwines,
                                mntfruits,
                                mntfishs,
                                mntsweets,
                                mntgolds,
                                mntmeats,
                                response,
                                complain,
                                recency,
                                total_campaigns_accepted,
                                total_spent) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

init_tables_query = [init_dim_customer, init_dim_date, init_fact_tables]