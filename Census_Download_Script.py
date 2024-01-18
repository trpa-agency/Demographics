import pandas as pd
import numpy as np
import requests
import pyodbc
import arcpy
from arcgis.features import FeatureLayer
# This is using Andy's Census API KEy
census_api_key = '9a73d08c296b844e58f1c70bd19c831826da5cbf'

# Need to define datatypes so that FIPS code doesn't get cast as int and drop leading 0s
dtypes = {
    'YEAR' : str,
    'STATE': str,
    'GEOGRAPHY': str,
    'GEOID': str,
    'TRPAID':str,
    'NEIGHBORHOOD': str
}

#Manually defined list of census tracts that are within the basin
 
service_url = 'https://maps.trpa.org/server/rest/services/Demographics/MapServer/27'

feature_layer = FeatureLayer(service_url)
tahoe_geometry_fields = ['YEAR', 'STATE', 'GEOGRAPHY', 'GEOID', 'TRPAID', 'NEIGHBORHOOD']
query_result = feature_layer.query(out_fields=",".join(tahoe_geometry_fields))
# Convert the query result to a list of dictionaries
feature_list = query_result.features

# Create a pandas DataFrame from the list of dictionaries
tahoe_geometry = pd.DataFrame([feature.attributes for feature in feature_list])

#Helper function that is used to concatenate census data return
def create_or_append_df(df, summary_df):
    if df.empty:
        df = summary_df.copy()
    else:
        df = pd.concat([df, summary_df])
    return df

#This gets the result of the get request and does some data wrangling to make it fit our structure
def get_request_census(request_url, sample_level, geo_name):
    response = requests.get(request_url)
    print(response.status_code)
    df = pd.DataFrame(response.json())
    #The json returns column names in the first row
    df.columns = df.iloc[0]
    df = df[1:]
    df['sample_level']=sample_level
    df['Geo_Name']=geo_name
    #Might as well add counties and states at this stage
    return df

def get_jobs_data(year, census_geom_year, variable, variablename, census_api_key, tahoe_geometry, variable_category):
    base_url = 'https://api.census.gov/data/'
    df_total=pd.DataFrame()
    #Formatting to match html get request
    #get the zipcodes for inclusion from tahoe_geometry
    zipcodes = tahoe_geometry['TRPAID'].loc[(tahoe_geometry['YEAR']==census_geom_year)&(tahoe_geometry['GEOGRAPHY']=='ZIP CODE')].str[:-4]
    print(zipcodes)
    
    for zipcode in zipcodes:
        #print(f'{base_url}/{year}/cbp?get={variable}&for={geometry_return}:*&in=state:{state}%20county:{county}{geometry_level}&key={census_api_key}')
        request_url = f'{base_url}{year}/cbp?get=GEO_ID,{variable}&for=zip%20code:{zipcode}&key={census_api_key}'
        print (request_url)
        response = requests.get(request_url)
        
        df = pd.DataFrame(response.json())
        #The json returns column names in the first row
        df.columns = df.iloc[0]
        df = df[1:]
        #Might as well add counties and states at this stage
        if df_total.empty:
            df_total=df
        else:
            df_total=pd.concat([df_total, df])

    #Figure out exactly what variable we want here
    #Add something here to handle margin of error
    df_total['variable_code']=variable
    df_total['variable_name']=variablename
    df_total['variable_category']= variable_category
    df_total['year_sample']=year
    df_total['sample_level']='ZIP CODE'
    df_total['dataset']= 'cbp'
    df_total['census_geom_year'] = census_geom_year
    df_total['GEO_ID'] = df_total['GEO_ID'].str.split('US').str[1]
    df_total['TRPAID'] = df_total['GEO_ID']+df_total['census_geom_year'].astype(str)
    df_total.columns.values[1] = 'value'
    df_total['value'] = df_total['value'].astype(float)
    df_total.insert(2, 'MarginOfError', np.NaN)
    return df_total


def get_variable_data(year, dataset, geometry_return, variable, variablename, census_api_key, census_geom_year, tahoe_geometry, variable_category):
    #Returns all data for a given dataset for Washoe, El Dorado, Carson City, Douglas, Placer Counties
    #Need to make five seperate api calls because of the geometry structure
    county_states ={
        '06': ['017','061'],
        '32': ['005', '031']
    }
    base_url = 'https://api.census.gov/data'
    df_total=pd.DataFrame()
    #Formatting to match html get request
    geometry_return=geometry_return.replace(" ", "%20")
    #This adds tract level to make block groups or blocks get request valid
    if geometry_return == 'tract':
        geometry_level = ''
    else:
        geometry_level='%20tract:*'
    if 'acs/acs5' in dataset:
        variable= variable +'E,'+variable + 'M'

    
    for state in county_states:
        for county in county_states[state]:
            print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for={geometry_return}:*&in=state:{state}%20county:{county}{geometry_level}&key={census_api_key}')
            request_url = f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for={geometry_return}:*&in=state:{state}%20county:{county}{geometry_level}&key={census_api_key}'
            response = requests.get(request_url)
            
            df = pd.DataFrame(response.json())
            #The json returns column names in the first row
            df.columns = df.iloc[0]
            df = df[1:]
            #Might as well add counties and states at this stage
            if df_total.empty:
                df_total=df
            else:
                df_total=pd.concat([df_total, df])
    #Figure out exactly what variable we want here
    #Add something here to handle margin of error
    df_total['variable_code']=variable
    df_total['variable_name']=variablename
    df_total['variable_category']= variable_category
    df_total['year_sample']=year
    df_total['sample_level']=geometry_return.replace("%20", " ")
    df_total['dataset']= dataset
    df_total['census_geom_year'] = census_geom_year
    df_total['GEO_ID'] = df_total['GEO_ID'].str.split('US').str[1]
    df_total['TRPAID'] = df_total['GEO_ID']+df_total['census_geom_year'].astype(str)
    df_total.columns.values[1] = 'value'
    df_total['value'] = df_total['value'].astype(float)
    if 'acs/acs5' in dataset:
        df_total.columns.values[2]='MarginOfError'
        df_total['variable_code'] = df_total['variable_code'].str.split(',').str[0]
    else:
        df_total.insert(2, 'MarginOfError', np.NaN)
    if geometry_return == 'tract':
        tract_col_loc = df_total.columns.get_loc('tract')
        df_total.insert(tract_col_loc, 'block group', np.NaN)

    #filter to just the tahoe parcels
    df_total = df_total[df_total['TRPAID'].isin(tahoe_geometry['TRPAID'])]
    df_total =  pd.merge(df_total, tahoe_geometry[['TRPAID', 'NEIGHBORHOOD']], on='TRPAID', how= 'left')
    
    return df_total

def get_non_tahoe_data(year,dataset, variable, variablename, census_api_key, census_geom_year, variable_category):
    base_url = 'https://api.census.gov/data'
    df_total=pd.DataFrame()
    county_states ={
        '06': ['017','061'],
        '32': ['005', '031', '510']
    }
    state_names={
        '06':'CA',
        '32':'NV'
    }
    county_names={
        '017':'El Dorado County',
        '061':'Placer County',
        '005':'Douglas County',
        '031':'Washoe County',
        '510':'Carson City County'
    }
    #Need to update this so that it handles the different years - are 2010 and 2020 the same?
    urban_centers = {
        'Reno-Sparks MSA':'39900',
        'Sacramento MSA': '40900',   
    }
    combined_metro_areas={
        'Sanfranciso CMSA': '488'
    }
    urban_centers_2000 = {
        'Reno-Sparks MSA':'6720',
        'Sacramento MSA': '6922',   
    }
    combined_metro_areas_2000={
        'Sanfranciso CMSA': '7362'
    }
    if year!="2000":
        for urban_center in urban_centers:
            urban_center_code = urban_centers[urban_center]
            print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{urban_center_code}&key={census_api_key}')
            request_url = f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{urban_center_code}&key={census_api_key}'            
            df = get_request_census(request_url,'MSA', urban_center)
            df_total = create_or_append_df(df_total, df)    
        for cma in combined_metro_areas:
            cma_code = combined_metro_areas[cma]
            print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=combined%20statistical%20area:{cma_code}&key={census_api_key}')
            request_url = f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=combined%20statistical%20area:{cma_code}&key={census_api_key}'
            df = get_request_census(request_url, 'MSA', cma)
            df_total = create_or_append_df(df_total, df)
    # for urban_center in urban_centers_2000:
    #         urban_center_code = urban_centers_2000[urban_center]
    #         statistical_region_url = f'metropolitan%20statistical%20area/micropolitan%20statistical%20area'
    #         print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for={statistical_region_url}:{urban_center_code}&key={census_api_key}')
    #         request_url= f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for={statistical_region_url}:{urban_center_code}&key={census_api_key}'
    #         df = get_request_census(request_url,'MSA',urban_center)
    #         df_total = create_or_append_df(df_total,df)
        
    for state in county_states:
        for county in county_states[state]:
            #https://api.census.gov/data/2010/dec/sf1?get=GEO_ID,P001001&for=county:017&in=state:06&key=9a73d08c296b844e58f1c70bd19c831826da5cbf
            print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=county:{county}&in=state:{state}&key={census_api_key}')
            request_url = f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=county:{county}&in=state:{state}&key={census_api_key}'
            countyname = county_names[county]
            df = get_request_census(request_url, 'County', countyname)
            df_total = create_or_append_df(df_total, df)
    for state in county_states:
        #https://api.census.gov/data/2010/dec/sf1?get=GEO_ID,P001001&for=county:017&in=state:06&key=9a73d08c296b844e58f1c70bd19c831826da5cbf
        print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=state:{state}&key={census_api_key}')
        request_url = f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=state:{state}&key={census_api_key}'
        geoname = state_names[state]
        df = get_request_census(request_url,'State', geoname)
        df_total = create_or_append_df(df_total, df)
        
    #Figure out exactly what variable we want here
    df_total['variable_code']=variable
    df_total['variable_name']=variablename
    df_total['variable_category']= variable_category
    df_total['year_sample']=year
    df_total['dataset']= dataset
    df_total['census_geom_year'] = census_geom_year
    df_total['GEO_ID'] = df_total['GEO_ID'].str.split('US').str[1]
    df_total['GEO_CODE'] = df_total['GEO_ID']+df_total['census_geom_year'].astype(str)
    df_total.columns.values[1] = 'value'
    return df_total

def census_download_wrapper (variable_file):
    dtypes = {
    'Variable' : str,
    'Code': str,
    'Category': str,
    'Datasource': str,
    'CodeNumber':str,
    'Year':str,
    'census_geom_year':str,
    'GeometryLevel':str
    }


    variables = pd.read_csv(variable_file,dtype=dtypes)

    #Loop through this?
    df_values=pd.DataFrame()
    for index, row in variables.iterrows():
        print(index)
        
        df = get_variable_data(row['Year'], row['Datasource Name'],row['GeometryLevel'],row['CodeNumber'],row['Variable'], census_api_key, row['census_geom_year'], tahoe_geometry, row['Category'])
        
        df_values = create_or_append_df(df_values, df)
    return df_values

def census_download_wrapper_non_tahoe(variable_file):
    dtypes = {
    'Variable' : str,
    'Code': str,
    'Category': str,
    'Datasource': str,
    'CodeNumber':str,
    'Year':str,
    'census_geom_year':str,
    'GeometryLevel':str
    }
    variables = pd.read_csv(variable_file,dtype=dtypes)

    #Loop through this?
    df_values=pd.DataFrame()
    for index, row in variables.iterrows():
        print(index)
        df = get_non_tahoe_data(row['Year'], row['Datasource Name'], row['CodeNumber'], row['Variable'], census_api_key, row['census_geom_year'], row['Category'])
        df_values = create_or_append_df(df_values, df)
    return df_values

def load_variable_multiple_year(year_range, dataset, geometry_return, variable, variablename, census_api_key, tahoe_geometry, variable_category):
    df=pd.DataFrame()
    df_return=pd.DataFrame()
    #year_range = [str(num) for num in range(year_start, year_end+1)]
    for year in year_range:
        if year in ['2020', '2021', '2022']:
            census_geom_year = '2020'
        else:
            census_geom_year = '2010'
        df = get_variable_data(year,dataset,geometry_return,variable,variablename,census_api_key, census_geom_year, tahoe_geometry, variable_category)
        print(len(df))
        df_return = create_or_append_df(df_return, df)
        
    return df_return


dashboard_fill = census_download_wrapper('Census_Variable_Lists\climate_dashboard_fill_in_3.csv')
dashboard_fill.to_excel('climate_dashboard_fill_in_3.xlsx')