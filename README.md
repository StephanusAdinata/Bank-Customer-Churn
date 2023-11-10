# Bank-Customer-Churn

<center><img src="https://www.cleartouch.in/wp-content/uploads/2022/11/Customer-Churn.png" height=350, width=700></img></center>

The primary objective of this project is to enhance the existing data pipeline by incorporating advanced tools and techniques for efficient data processing, validation, and visualization. The project focuses on improving data reliability, ensuring data quality, and enhancing the overall data workflow.

# Tools 
- Pandas
- Psycopg2
- Elasticsearch
- Kibana
- Apache Airflow
- Great Expectation
- SQL

# File / Folder Overview

P2M2_Stephanus_data_clean.csv = CSV cleaned file
P2M2_Stephanus_data_raw.csv = CSV default/raw file
Stephanus_DAG = Function for cleaning, import, scheduling using apache airflow
Stephanus_DDL = SQL Query
Stephanus_GX = Validation function using Great Expectation
Image (Folder) = Screenshoot of visualization and insight using Kibana
