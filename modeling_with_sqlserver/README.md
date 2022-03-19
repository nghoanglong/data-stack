# Marketing Analytics data ETL process

Dựa trên bộ dataset: https://www.kaggle.com/datasets/jackdaoud/marketing-data

Nhiệm vụ chính trong project này ta sẽ viết automation code để tự động ETL dữ liệu vào những bảng dimension và fact trong Data Warehouse

Việc xây dựng Data Warehouse cho bộ dữ liệu trên giúp khám phá ra những thông tin quan trọng, giúp doanh nghiệp cải thiện độ chính xác và doanh thu khi Marketing đến tập khách hàng có sẵn

Những công cụ sử dụng:
+ Python, Pandas
+ Jupyter Notebook
+ Pyodbc driver
+ MSSQL Server 2017 (docker)

## Running the ETL

Bước 1: Cài đặt docker và pull image MSSQL Server 2017
```
docker pull mcr.microsoft.com/mssql/server:2017-latest
```

Bước 2: Run MSSQL Server image
```
docker run -it --name=sqlserver -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=dienpasswordmongmuonvaoday" -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest
```

Bước 3: Run ETL


```
python MarketingAnalytics_DAL.py
```

## Database Schema Design

### Customer table

- *Type:* Dimension table

| Column | Type |
| ------ | ---- |
| `user_id` | `INT NOT NULL, PRIMARY KEY` 
| `year_birth` | `INT`
| `education` | `VARCHAR(255)` 
| `marital_status` | `VARCHAR(255)` 
| `income` | `INT` 
| `kidhome` | `INT` 
| `teenhome` | `INT`
| `country` | `VARCHAR(255)`  |

### Date table

- *Type:* Dimension table

| Column | Type 
| ------ | ---- 
| `date_id` | `DATE NOT NULL, PRIMARY KEY` 
| `year_enroll` | `INT` 
| `month_enroll` | `INT` 
| `day_enroll` | `INT` 


### Fact table

- *Type:* Fact table

| Column | Type 
| ------ | ---- 
| `user_id` | `INT NOT NULL, PRIMARY KEY` 
| `date_id` | `DATE NOT NULL, PRIMARY KEY` 
| `numdealspurchases` | `INT`
| `numwebpurchases` | `INT` 
| `numcatalogpurchases` | `INT`
| `numstorepurchases` | `INT`
| `numwebvisitmonth` | `INT`
| `mntwines` | `FLOAT`
| `mntfruits` | `FLOAT`
| `mntfishs` | `FLOAT`
| `mntsweets` | `FLOAT`
| `mntgolds` | `FLOAT`
| `mntmeats` | `FLOAT`
| `response` | `INT`
| `complain` | `INT`
| `recency` | `INT`
| `total_campaigns_accepted` | `INT`
| `total_spent` | `FLOAT`

## The project file structure

 - `marketing_data.csv` - file dataset được down từ trang chủ
 - `preprocess_data.py` - Xử lý dữ liệu
 - `queries.py` - Lưu trữ các lệnh thao tác trên database
 - `MarketingAnalytics_DAL.py` - File chính ETL dữ liệu và initialize tables 
 - `preprocess_data.ipynb` - Xử lý dữ liệu trên jupyter notebook
