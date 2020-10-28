import datetime
from datetime import datetime,timedelta



# Func to Create SQL Query using predefined templates 
# Input - Table & Query Configurations
# Output - SQL Query
def create_sql_query(props,database,table,full_refresh_flag):

	if(props.get('select_all') == True):

		if(full_refresh_flag == True):

			sql =  'select * from {}.{} where {} < {}'
			filter_column = props.get('filter1')

			end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y:%m:%d')

			sql = sql.format(database,table,filter_column,end_date)
			return sql

		else:

			sql =  'select * from {}.{} where {} between {} and {} '
			filter_column = props.get('filter1')

			start_date = datetime.strftime(datetime.now() - timedelta(2), '%Y:%m:%d')
			end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y:%m:%d')

			sql = sql.format(database,table,filter_column,start_date,end_date)
			return sql

	else :
		column_list = props.get('select_columns')
		if(full_refresh_flag == True):

			sql = 'select ' + ', '.join(column_list) + ' '
			sql = sql+" from {}.{} where {} between < {}"

			filter_column = props.get('filter1')
			end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y:%m:%d')

			sql = sql.format(database,table,filter_column,end_date)

			return sql

		else:
			sql = 'select ' + ', '.join(column_list) + ' '
			sql = sql+" from {}.{} where {} between {} and {}"

			filter_column = props.get('filter1')
			start_date = datetime.strftime(datetime.now() - timedelta(2), '%Y:%m:%d')
			end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y:%m:%d')

			sql = sql.format(database,table,filter_column,start_date,end_date)
			return sql