'''
 AUTOMATING THE ENTIRE WORKFLOW USING APACHE AIRFLOW
=====================================================

Steps :
    1.Creating a folder in Hadoop HDFS
    2.Uploading the Dataset to the created folder in HDFS
    3.Creating a table in Hive which is compactable with the Dataset
    4.Extracting the data from HDFS to the table created in Hive
    5.Connecting Hive and Spark and doing analysis using Spark SQL
        Sales Statistics
        Top Selling Product
        Top Product Category
        Top Performing Sales Representative
        Sales by City
    6.Uploading the analysed data back to HDFS
'''
#======================================================================

# Libraries
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

#----------------------------------------------------------------------

default_arg ={
    'owner':'Rizwan',
    'start_date':datetime(2024,4,30)
}

dag = DAG(
    'Data_Engineering_Project_Using_Big_Data_Technologies',
    default_args = default_arg,
    description = 'Data Extraction, Analysis and automation using Hadoop-HDFS, Hive, Spark-Spark SQL and Airflow',
    schedule_interval = None,
    catchup = False
)

#----------------------------------------------------------------------

# 1.Creating a folder in Hadoop HDFS

folder_creation_cmd_HDFS = "hadoop fs -mkdir /Data_Engineering_Project_HDFS"

create_folder_HDFS_task = BashOperator(
    task_id = 'create_folder_in_HDFS',
    bash_command = folder_creation_cmd_HDFS,
    dag = dag
)

#----------------------------------------------------------------------

# 2.Uploading the Dataset to the created folder in HDFS

upload_data_cmd_HDFS = "hadoop fs -put /home/rizwan/Downloads/Sales_Data.csv /Data_Engineering_Project_HDFS"

upload_data_HDFS_task = BashOperator(
    task_id = 'upload_data_to_HDFS',
    bash_command = upload_data_cmd_HDFS,
    dag = dag
)

#----------------------------------------------------------------------

# 3.Creating a table in Hive which is compactable with the Dataset

hive_table_creation_cmd = """
hive -e "CREATE TABLE sales_data_table (
    dte STRING,
    product STRING,
    category STRING,
    sales_rep STRING,
    city STRING,
    no_of_units INT,
    price DOUBLE,
    amount DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ('skip.header.line.count'='1');"
"""

hive_table_creation_task = BashOperator(
    task_id = 'hive_table_creation',
    bash_command = hive_table_creation_cmd,
    dag = dag
)

#----------------------------------------------------------------------
 
 # 4.Extracting the data from HDFS to the table created in Hive 

load_data_HDFS_to_Hive_cmd = """
hive -e "LOAD DATA INPATH '/Data_Engineering_Project_HDFS/Sales_Data.csv' INTO TABLE sales_data_table;"
"""

load_data_HDFS_to_Hive_task = BashOperator(
    task_id ='load_data_from_HDFS_to_sales_data_table_Hive',
    bash_command = load_data_HDFS_to_Hive_cmd,
    dag = dag
)

#----------------------------------------------------------------------

# Task Dependencies :

create_folder_HDFS_task >> upload_data_HDFS_task >> hive_table_creation_task \
>> load_data_HDFS_to_Hive_task