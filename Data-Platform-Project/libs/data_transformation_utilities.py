import pandas as pd
import json
from pandas.io.json import json_normalize 
from datetime import datetime,timedelta


# Function to Flatten JSON Columns to Produced Normalised dataframe
# Input - Dataframe , List of Columns to be Flatenned , Json Level to flatten till
# Output - Parsed Dataframe
def flatten_json_data(df,flatten_columns,flatten_max_level):
	try:
		initial_df = df
		for i,val in enumerate(flatten_columns):
			
			col = '{}'.format(str(val))

			for index, row in df.iterrows():
				dicts =json.loads(df[col][index])
				parsed_df = json_normalize(dicts,sep = '_',max_level = flatten_max_level)

				if(index == 0):
					master_df = parsed_df
					
				elif(index > 0):
					master_df = pd.concat([master_df,parsed_df],ignore_index = True)
			
			df = pd.concat([df,master_df],ignore_index = True)
		
		# Return Flattened DF if Transformation is succesfull
		return df
        
	except Exception as e:
		print(" Not able to parse json due to error - ",str(e))
		print("Returning initial df")
		return initial_df
