import pandas as pd ,re

# These are utility functions which reads data from files or initialize data by themselves to create static data for dimention tables

# Read , Clean & Model data for Country Dimention
def fetch_country_names():
    try:
        column_names = ['country_code','country_name']
        country_dict = {}
        with open('/home/hadoop/extra_data/country_codes.txt') as f:
             for line in f:
                a = re.findall(r'\d{1,4}',line)
                b = re.findall(r'\'(.*)\'.*',line)
                country_dict[a[0]]= b[0]

        country_df = pd.DataFrame(list(country_dict.items()),columns = ['country_code','country_name'])
        return country_df
    except Exception as e:
        print(str(e))
        country_df = pd.DataFrame(columns = columns_names)
        return country_df

# Read , Clean & Model data for Visa Type Dimention
def fetch_visa_type():
    try:
        columns_names = ['visa_code','visa_type']
        
        visa_type = {}
        with open('/home/hadoop/extra_data/Visa_type.txt') as f:
             for line in f:
                a = re.findall(r'\d{1}',line)
                b = re.findall(r'[A-z].*',line)
                #print(a,"",b)
                visa_type[a[0]]= b[0]
                print(a[0],b[0])
        #visa_type_df = pd.DataFrame.from_dict(visa_type,orient = 'index')
        visa_type_df = pd.DataFrame(list(visa_type.items()),columns = columns_names)
        #print(visa_type)    
        return visa_type_df
    
    except Exception as e:
        print(str(e))
        visa_type_df = pd.DataFrame(columns = columns_names)
        return visa_type_df
    
# Read , Clean & Model data for Port Dimention
def fetch_port_details():
    try:
        column_names = ['port_code','port_full_name']
        port_dict = {}
        with open('/home/hadoop/extra_data/port_details.txt') as f:
            for line in f:
                a = re.findall(r'\'(.*)\'.*\'(.*)\,',line)
                port_dict[a[0][0]]=a[0][1]
            port_details_df = pd.DataFrame(list(port_dict.items()),columns = column_names)
            return port_details_df
    except Exception as e:
        print(str(e))
        port_details_df = pd.DataFrame(columns = column_names)
        return port_details_df

# Prepare Data for Visa Mode Dimention
def I94_mode_details():
    column_names = ['id','mode']
    
    dict = {
    "1": "Air",
    "2": "Sea",
    "3": "Land",
    "9": "Not reported"
    }
    mode_details_df = pd.DataFrame(list(dict.items()),columns = column_names)
    return mode_details_df

# Prepare Data for Month Dimention
def create_month_df():
    try:
        
        month_names=["january","february","march","april","may","june","july","august","september","october","november","december"]
        month_dict = {}
        for i,m in enumerate(month_names):
            month_dict[i+1] = m
            
        month_df = pd.DataFrame(data=list(month_dict.items()),columns=["month_code","month_name"])
        
        return month_df
        
    except Exception as e:
        print(str(e))
    