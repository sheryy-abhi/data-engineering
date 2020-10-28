import airflow
from airflow import DAG
from airflow import AirflowException
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook


import boto3
import yaml
import gzip
import json
import pandas as pd
from pandas.io.json import json_normalize 
from io import StringIO
from io import BytesIO, TextIOWrapper
from datetime import datetime, timedelta, time


import libs.common_utilities as common_utils
import libs.query_builder as query_builder
import libs.data_transformation_utilities as data_utils

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}


dag = DAG(
    "Data_Platform_ETL",
    schedule_interval="@daily",
    default_args=args)




process_start = DummyOperator(
	task_id='process_start',
	dag=dag
)



# Function to perform ETL task for MySQL Table
def fetch_data_from_MySQL(table_name,props,database,s3_bucket,s3_prefix ,**kwargs):
	try:
		table_exec_config = Variable.get("table_run_config", deserialize_json=True)

		if( table_name not in table_exec_config):
			print("Table not  exists")
			full_refresh_flag = True

		else :
			print("Table exists")
			full_refresh_flag = False

		print("fetching data from Mysql")
		df = common_utils.load_data_from_mysql_table_to_pandas(database,table_name,props,full_refresh_flag)

		if(props.get('json_flatten_flag') == True):
			#Flatten JSON Data and recreate DF with added columns
			df =data_utils.flatten_json_data(df,props.get('flatten_columns'),props.get('json_flatten_level'))

		print("Building s3 path for the file to be written in Data Lake")
		running_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
		table_type = props.get('type')

		s3_path = '{}/{}/{}/{}/{}/{}.csv.gz'
		s3_path = s3_path.format(s3_prefix,database,table_type,table_name,running_date,table_name)

		# Full S3 path looks like this -
		# s3://data-platform-s3-bucket/data-lake/analytics-db/dynamic/transactions/2020-10-28/transactions.csv

		delimiter = props.get('delimiter')

		print("Writing Data to S3")
		common_utils.push_data_to_s3(df,s3_bucket,s3_path,delimiter)
		
		print("ETL complete for table ",table_name," at ",str(datetime.now()))

		# Update the table_run_config Airflow Variable to identfy that this table is pre-exixting now and will only run on incremental data
		if(full_refresh_flag == True):

			# Append the newly added table name in config dict with flag 1
			table_exec_config[table_name] = 1
			# By this it will be identfyble that this table doesn't needs a full refresh

			# Check if the Structure of JSON is fine to be put as Variable
			if isinstance(table_exec_config, dict):
				Variable.set("table_run_config", table_exec_config, serialize_json=True)

			else : raise AirflowException("Wrong Json Structure of Table Execution Config Airflow Variable")
	
	except Exception as e:
		print(str(e))
		raise AirflowException(" ETL Failed for table - ",table_type," of database ",database)

	


# Function to create Dynamic tasks
def create_dynamic_tasks(task_id, callableFunction, args):
	task = PythonOperator(
		task_id = task_id,
		provide_context=True,
		python_callable = eval(callableFunction),
		op_kwargs = args,
		xcom_push = True,
		dag = dag,
	)
	return task

# End Process Operator
process_end = DummyOperator(
	task_id='process_end',
	dag=dag)


with open('/home/abhisheak/airflow/config.yml') as f:
	# Load ETL config from YAML file
	configFile = yaml.safe_load(f)
	
	#Extract table configurations
	tables = configFile['tables']
	s3_bucket = configFile['bucket_name']
	s3_prefix = configFile['prefix']
	db_name = configFile['dbname']
	
	#Create Task for each table defined in YAML file
	for table in tables:
		for table_name,props in table.items():
			
			# Create Task Dynamically
			get_sql_data_task = create_dynamic_tasks('{}-fetch_data_from_MySQL'.format(table_name), 
											'fetch_data_from_MySQL', 
											{'table_name':table_name,'props': props, 'dbname': db_name,'s3_bucket':s3_bucket,'s3_prefix':s3_prefix})
			
			# Add sequence of tasks
			process_start >> get_sql_data_task
			get_sql_data_task >> process_end