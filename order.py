from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import pyspark
import boto3
from io import StringIO
from pyspark.sql import SparkSession
import datetime as dt
#from airflow.contrib.hooks.spark_sql_hook import SparkSqlHook
# default arguments for each task
default_args = {
    'owner': 'fba',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG('olist__order_etl_pipeline',
          default_args=default_args,
          schedule_interval=None)  # "schedule_interval=None" means this dag will only be run by external commands
spark = SparkSession \
    .builder \
    .appName("SQL Query") \
    .getOrCreate()
TEST_BUCKET = 'deneme-commerce'
TEST_KEY = 'olist_orders_dataset.csv'
LOCAL_FILE = '/tmp/order.csv'
s3 = boto3.resource('s3',aws_access_key_id="AKIA2VYVXVOH2YXJNEVP",
         aws_secret_access_key= "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")
# simple download task
def download_file(bucket, key, destination):
    
    
    s3.meta.client.download_file(bucket, key, destination)
    
def download_file2(bucket, key, destination):
    
  
    s3.meta.client.download_file(bucket, key, destination)

def download_file3(bucket, key, destination):
    
    s3.meta.client.download_file(bucket, key, destination)
    
def clean_data():
    df = pd.read_csv('/tmp/order.csv')

    columns_timestamp = ['order_purchase_timestamp','order_approved_at',
                     'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    for column in columns_timestamp:
        df[column] = pd.to_datetime(df[column])

        df['diff_delivery_days'] = (pd.to_datetime(df['order_estimated_delivery_date']) - pd.to_datetime(df['order_delivered_customer_date'])).dt.days
    print("Before preprocessing: ")
    print(df.isnull().sum())
    print(df["order_status"].value_counts())
    df = df[df["order_status"]=="delivered"]
    df["order_approved_at"] = df["order_approved_at"].fillna(df["order_purchase_timestamp"])
    print("After preprocessing: ")
    print(df.isnull().sum())   
    df.dropna(subset=["order_approved_at","order_delivered_carrier_date","order_delivered_customer_date"],axis=0,inplace=True)
    df.to_csv('/tmp/clean_order.csv')
    print(df.isnull().sum())   
    
def use_file():
    df1=spark.read.csv('/tmp/order_item.csv',header=True)
    
    df2 = spark.read.csv('/tmp/clean_order.csv',header=True)
    #df2.printSchema()
    df3 = spark.read.csv('/tmp/seller.csv', header=True)
    df1.createOrReplaceTempView("order_item")
    df2.createOrReplaceTempView("order")
    df3.createOrReplaceTempView("seller")
    sqlDF = spark.sql("SELECT ord.order_id,f.seller_id,s.seller_city,ord.order_estimated_delivery_date, ord.order_delivered_carrier_date FROM order ord \
 			 INNER JOIN order_item f ON ord.order_id = f.order_id \
 			 INNER JOIN seller s ON f.seller_id = f.seller_id \
 			 WHERE ord.order_delivered_carrier_date> ord.order_estimated_delivery_date")

    sqlDF.show()
    sqlDF.toPandas().to_csv('mycsv.csv')
    s3.meta.client.upload_file('mycsv.csv', TEST_BUCKET, 'resultdf.csv')
   


download_order_data = PythonOperator(
    task_id='download_order_data',
    python_callable=download_file,
    op_kwargs={'bucket': TEST_BUCKET, 'key': TEST_KEY, 'destination': LOCAL_FILE},
    dag=dag)
    

download_sellers_data = PythonOperator(
    task_id='download_sellers_data',
    python_callable=download_file2,
    op_kwargs={'bucket': TEST_BUCKET, 'key': 'olist_sellers_dataset.csv', 'destination': '/tmp/seller.csv'},
    dag=dag)
    
download_orderitem_data = PythonOperator(
    task_id='download_orderitem_data',
    python_callable=download_file3,
    op_kwargs={'bucket': TEST_BUCKET, 'key': 'olist_order_items_dataset.csv', 'destination': '/tmp/order_item.csv'},
    dag=dag)

upload_result_to_S3 = PythonOperator(
    task_id='upload_result_to_S3',
    python_callable=use_file,
   dag=dag)
   
clean_s3_file = PythonOperator(
    task_id='clean_s3_file',
    python_callable=clean_data,
   dag=dag)
[download_order_data,download_sellers_data,download_orderitem_data] >> clean_s3_file >> upload_result_to_S3
