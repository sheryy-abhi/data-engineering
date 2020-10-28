import boto3
import yaml
import gzip
import json
import pandas as pd
from pandas.io.json import json_normalize 
from io import StringIO
from io import BytesIO, TextIOWrapper
from datetime import datetime, timedelta, time

import airflow
from airflow import AirflowException
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook

import data_transformation_utilities as data_utils
import query_builder as query_builder


# Function to Load data from MySQL DB 
# Input- Table Configurations
# Output- Data Frame
def load_data_from_mysql_table_to_pandas(database,table_name,props,full_refresh_flag):
	try:
        # TO DO  ## Have to Replace mysql_conn_id with actual MySQL DB connecton added in Airflow connection
		hook = MySqlHook(mysql_conn_id="mysql_conn_id").get_conn()
		sql = query_builder.create_sql_query(props,database,table_name,full_refresh_flag)
		df = pd.read_sql(sql,hook)

		return df

	except Exception as e:
		print(str(e))
		raise AirflowException('Failed To read Data from S3')



# Func to Push Data to s3
# Input - Dataframe & S3 Path
# Note - This only works if server have neccesary S3 IAM permetions
def push_data_to_s3(df,s3_bucket,s3_file_path,delimiter):
	try:
		gz_csv_buffer = BytesIO()
		with gzip.GzipFile(mode='w', fileobj=gz_csv_buffer) as gz_file:
			df.to_csv(TextIOWrapper(gz_file, 'utf8'),sep=delimiter, index=False)

		s3_resource = boto3.resource('s3')
		s3_resource.Object(s3_bucket, s3_file_path).put(Body=gz_csv_buffer.getvalue())
		
		print("Written {} records to {}".format(len(df), s3_file_path))

	except Exception as e:
		print(str(e))
		raise AirflowException(' Exception occured in Writing File to S3')


