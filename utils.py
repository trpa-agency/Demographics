import pandas as pd
import numpy as np
import requests
import pyodbc
import arcpy
import time
from arcgis.features import FeatureLayer
from arcgis.gis import GIS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os


# get feature service as dataframe
def get_fs_as_df(url: str):
    layer = FeatureLayer(url, gis=GIS()) 
    return pd.DataFrame.spatial.from_layer(layer)

# Gets data from the TRPA server
def get_fs_data(service_url):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query()
    # Convert the query result to a list of dictionaries
    feature_list = query_result.features
    # Create a pandas DataFrame from the list of dictionaries
    all_data = pd.DataFrame([feature.attributes for feature in feature_list])
    # return data frame
    return all_data

def get_tahoe_geometry(
    service_url='https://maps.trpa.org/server/rest/services/Demographics/MapServer/27',
    geometry_fields=None
):
    """
    Query a FeatureLayer and return selected fields as a pandas DataFrame.
    """
    if geometry_fields is None:
        geometry_fields = ['YEAR', 'STATE', 'GEOGRAPHY', 'GEOID', 'TRPAID', 'NEIGHBORHOOD']

    feature_layer = FeatureLayer(service_url)
    
    query_result = feature_layer.query(
        out_fields=",".join(geometry_fields)
    )
    
    feature_list = query_result.features
    
    tahoe_geometry = pd.DataFrame(
        [feature.attributes for feature in feature_list]
    )
    
    return tahoe_geometry

#This gets the result of the get request and does some data wrangling to make it fit our structure
def get_request_census(request_url, sample_level, geo_name, session):
    response = session.get(request_url)
    print(response.status_code)
    df = pd.DataFrame(response.json())
    #The json returns column names in the first row
    df.columns = df.iloc[0]
    df = df[1:]
    df['sample_level']=sample_level
    df['Geo_Name']=geo_name
    #Might as well add counties and states at this stage
    return df

def get_variable_data(
    year,
    dataset,
    geometry_return,
    variable,
    variablename,
    census_api_key,
    census_geom_year,
    tahoe_geometry,
    variable_category,
    session
):
    county_states = {
        '06': ['017', '061'],
        '32': ['005', '031']
    }

    base_url = 'https://api.census.gov/data'
    dfs = []

    geometry_return = geometry_return.replace(" ", "%20")

    if geometry_return == 'tract':
        geometry_level = ''
    else:
        geometry_level = '%20tract:*'

    if 'acs/acs5' in dataset:
        variable = f"{variable}E,{variable}M"

    for state in county_states:
        for county in county_states[state]:

            request_url = (
                f"{base_url}/{year}/{dataset}"
                f"?get=GEO_ID,{variable}"
                f"&for={geometry_return}:*"
                f"&in=state:{state}%20county:{county}{geometry_level}"
                f"&key={census_api_key}"
            )

            print(request_url)

            for attempt in range(3):
                try:
                    response = session.get(request_url, timeout=60)
                    break
                except requests.exceptions.ConnectionError:
                    if attempt == 2:
                        raise
                    time.sleep(2 ** attempt)

            if response.status_code != 200:
                raise RuntimeError(
                    f"Census API error {response.status_code} for {request_url}"
                )

            try:
                payload = response.json()
            except ValueError:
                raise RuntimeError(
                    f"Non-JSON response from Census API:\n{response.text[:300]}"
                )

            # Census sometimes returns header-only or error payloads
            if not isinstance(payload, list) or len(payload) <= 1:
                continue


            df = pd.DataFrame(payload)
            df.columns = df.iloc[0]
            df = df.iloc[1:].copy()

            if not df.empty:
                dfs.append(df)

            # rate limiting protection
            time.sleep(1.0)

    if not dfs:
        return pd.DataFrame()

    df_total = pd.concat(dfs, ignore_index=True)

    # ---- metadata fields ----
    df_total['variable_code'] = variable
    df_total['variable_name'] = variablename
    df_total['variable_category'] = variable_category
    df_total['year_sample'] = year
    df_total['sample_level'] = geometry_return.replace("%20", " ")
    df_total['dataset'] = dataset
    df_total['census_geom_year'] = census_geom_year

    # ---- GEOID handling ----
    df_total['GEO_ID'] = df_total['GEO_ID'].str.split('US').str[1]
    df_total['TRPAID'] = df_total['GEO_ID'] + df_total['census_geom_year'].astype(str)

    # ---- value / MOE handling ----
    df_total.columns.values[1] = 'value'
    df_total['value'] = pd.to_numeric(df_total['value'], errors='coerce')

    if 'acs/acs5' in dataset:
        df_total['variable_code'] = df_total['variable_code'].str.split(',').str[0]

        # ACS does NOT guarantee MOE exists
        if df_total.shape[1] > 2:
            possible_moe_col = df_total.columns[2]
            if possible_moe_col not in ('state', 'county', 'tract', 'block group'):
                df_total.rename(
                    columns={possible_moe_col: 'MarginOfError'},
                    inplace=True
                )
                df_total['MarginOfError'] = pd.to_numeric(
                    df_total['MarginOfError'], errors='coerce'
                )
            else:
                df_total.insert(2, 'MarginOfError', np.nan)
        else:
            df_total.insert(2, 'MarginOfError', np.nan)
    else:
            df_total.insert(2, 'MarginOfError', np.nan)

    if geometry_return == 'tract':
        tract_col_loc = df_total.columns.get_loc('tract')
        df_total.insert(tract_col_loc, 'block group', np.nan)

    # ---- Tahoe filter + join ----
    df_total = df_total[df_total['TRPAID'].isin(tahoe_geometry['TRPAID'])]

    df_total = pd.merge(
        df_total,
        tahoe_geometry[['TRPAID', 'NEIGHBORHOOD']],
        on='TRPAID',
        how='left'
    )

    return df_total

# This doesn't handle getting disconnected by the census server
def census_download_wrapper(variables_df, year, tahoe_geometry, census_api_key, census_geom_year):
    dfs = []

    for index, row in variables_df.iterrows():
        print(index)

        df = get_variable_data(
            year=year,
            dataset=row['dataset'],
            geometry_return=row['sample_level'],
            variable=row['variable_code'],
            variablename=row['variable_name'],
            census_api_key=census_api_key,
            census_geom_year=census_geom_year,
            tahoe_geometry=tahoe_geometry,
            variable_category=row['variable_category']
        )

        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)
def get_existing_variables(year, dataset, feature_layer_url):
    #returns unique variable, variable_name, category and sample level of all existing variables
    #Need to figure out how to handle ones we post-process - maybe this should be a field in the table? Currently it's variable_category
    data = get_fs_as_df(feature_layer_url)
    data = data[data['year_sample']==year]
    data = data[data['dataset']==dataset]
    existing_vars = data[['variable_code', 'variable_name', 'variable_category', 'sample_level','dataset']].drop_duplicates()
    existing_vars['variable_code'] = existing_vars['variable_code'].str[:-1]
    return existing_vars

def census_download_wrapper_checkpointed(
    variables_df,
    year,
    checkpoint_dir,
    final_output_csv,
    tahoe_geometry,
    session,
    census_geom_year=2020,    
    census_api_key=None
    ):
    os.makedirs(checkpoint_dir, exist_ok=True)

    completed = {
        f.replace(".csv", "")
        for f in os.listdir(checkpoint_dir)
        if f.endswith(".csv")
    }

    all_dfs = []

    for index, row in variables_df.iterrows():
        checkpoint_name = f"{year}_{row['variable_code']}_{row['sample_level']}"
        checkpoint_path = os.path.join(checkpoint_dir, f"{checkpoint_name}.csv")

        if checkpoint_name in completed:
            print(f"Skipping completed: {checkpoint_name}")
            df = pd.read_csv(checkpoint_path, dtype=str)
            all_dfs.append(df)
            continue

        print(f"Processing {checkpoint_name}")

        try:
            df = get_variable_data(
                year=year,
                dataset=row['dataset'],
                geometry_return=row['sample_level'],
                variable=row['variable_code'],
                variablename=row['variable_name'],
                census_api_key=census_api_key,
                census_geom_year=census_geom_year,
                tahoe_geometry=tahoe_geometry,
                variable_category=row['variable_category']
            )

            if df.empty:
                print(f"No data returned for {checkpoint_name}")
                continue

            df.to_csv(checkpoint_path, index=False)
            all_dfs.append(df)

        except Exception as e:
            print(f"FAILED {checkpoint_name}")
            raise

    if not all_dfs:
        return pd.DataFrame()

    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df.to_csv(final_output_csv, index=False)

    return final_df

def make_session():
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=1,
        pool_maxsize=1
    )

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


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
    dfs = []
    if year!="2000":
        for urban_center in urban_centers:
            urban_center_code = urban_centers[urban_center]
            print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{urban_center_code}&key={census_api_key}')
            request_url = f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{urban_center_code}&key={census_api_key}'            
            df = get_request_census(request_url,'MSA', urban_center)
            dfs.append(df)
        for cma in combined_metro_areas:
            cma_code = combined_metro_areas[cma]
            print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=combined%20statistical%20area:{cma_code}&key={census_api_key}')
            request_url = f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for=combined%20statistical%20area:{cma_code}&key={census_api_key}'
            df = get_request_census(request_url, 'MSA', cma)
            dfs.append(df)
    # for urban_center in urban_centers_2000:
    #         urban_center_code = urban_centers_2000[urban_center]
    #         statistical_region_url = f'metropolitan%20statistical%20area/micropolitan%20statistical%20area'
    #         print(f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for={statistical_region_url}:{urban_center_code}&key={census_api_key}')
    #         request_url= f'{base_url}/{year}/{dataset}?get=GEO_ID,{variable}&for={statistical_region_url}:{urban_center_code}&key={census_api_key}'
    #         df = get_request_census(request_url,'MSA',urban_center)
    #         df_total = create_or_append_df(df_total,df)

def calculate_median_value(df, bin_column, sort_column, count_column, category_field, category,grouping_variables, cumulative_sorting_variables):
        # Create a new DataFrame to avoid modifying the original one
    #change value to count column
    #Do we need to handle excluding non-tahoe things here or should we do it in the input?
    summary_df = df.copy()
    summary_df[count_column]=summary_df[count_column].astype(int)
    #summary_df=summary_df.loc[summary_df[count_column]!='510']
    summary_df = summary_df.groupby(grouping_variables)[count_column].sum()
    summary_df = summary_df.reset_index()
    #This handles -6666 values that they sometimes add for unknown data
    summary_df = summary_df.loc[summary_df[count_column]>=0]
    
    
    summary_df= summary_df.loc[summary_df[category_field]==category]
    # Sort the DataFrame based on the variable name column
    # This depends on the fact that census variables start with the lowest value and go up 
    #This needs to all be rethought to have some kind of window function to handle multiple years
    summary_df.sort_values(by=sort_column, inplace=True)
    summary_df = summary_df.reset_index()
    
    # Extract lower and upper limits from bin categories
    #This uses regex to find numbers and removes commas to make numbers numbers 
    pattern = r'(\d+[\d,]*)'
    summary_df['temp'] = summary_df[bin_column].str.replace(',', '').str.findall(pattern)
    #Looks for values that have two numbers and puts empty placeholders for the ones that only have one (upper and lower)
    summary_df[['Lower', 'Upper']] = summary_df['temp'].apply(lambda x: pd.Series(x[:2]) if len(x) == 2 else pd.Series([None, None]))
    summary_df['Lower'] = summary_df['Lower'].astype(float)
    summary_df['Upper'] = summary_df['Upper'].astype(float)
    # Handle first category
    
    first_upper = float(summary_df['temp'].iloc[0][0])
    low_variable_name = summary_df[bin_column].iloc[0]

    summary_df.loc[summary_df[bin_column]==low_variable_name,'Lower'] = 0  # Set lower value to 0
    summary_df.loc[summary_df[bin_column]==low_variable_name,'Upper'] = first_upper

    
    # Handle last category
    
    last_lower = float(summary_df['temp'].iloc[-1][0])
    upper_variable_name = summary_df[bin_column].iloc[-1]
    summary_df.loc[summary_df[bin_column]==upper_variable_name,'Lower'] = last_lower
    summary_df.loc[summary_df[bin_column]==upper_variable_name,'Upper'] = np.inf  # Set upper value to infinity
    summary_df[count_column]= summary_df[count_column].astype(float)   
    # Calculate cumulative count
    cumulative_grouping_variables = grouping_variables

    cumulative_grouping_variables.remove(bin_column)
    cumulative_grouping_variables.remove(sort_column)

    #Update this to be parameterized
    
    summary_df.sort_values(by=cumulative_sorting_variables, inplace=True)
    summary_df = summary_df.reset_index()
    summary_df['cumulative_sum'] = summary_df.groupby(cumulative_grouping_variables, as_index=False)[count_column].cumsum()
    summary_df['TotalSum'] = summary_df.groupby(cumulative_grouping_variables, as_index=False)[count_column].transform('sum')
    summary_df['previous_cumulative'] = summary_df['cumulative_sum'].shift()

    summary_df = summary_df.loc[summary_df['cumulative_sum']>=(summary_df['TotalSum']/2)].groupby(cumulative_grouping_variables, as_index=False).first()

    summary_df['cumulative_difference'] = summary_df['TotalSum']  / 2 - summary_df['previous_cumulative']
    summary_df['interpolation_ratio'] = summary_df['cumulative_difference'] /  (summary_df['cumulative_sum']- summary_df['previous_cumulative'])
    summary_df['median_value'] = summary_df['Lower'] + summary_df['interpolation_ratio'] * (summary_df['Upper'] - summary_df['Lower'])
    
    
    return summary_df


def categorize_values(census_df, category_csv, category_column, grouping_prefix):
    categories = pd.read_csv(category_csv)    
    census_df['value'] = census_df['value'].astype(float)
    joined_data = census_df.merge(categories, on = 'variable_code', how = 'left')
    joined_data.sort_values(by='variable_code', inplace=True)
    #This will get rid of any extra columns in the category_csv
    group_columns = [column for column in census_df if column not in ['value', 'variable_code', 'variable_name', 'MarginOfError','OBJECTID']]
    group_columns.append(category_column)
    #grouped_data = joined_data.groupby(group_columns, as_index=False)['value'].sum()    
    print(group_columns)
    grouped_data = joined_data.groupby(group_columns, as_index=False, dropna=False).agg({'value':'sum',
                                                                           'variable_code':lambda x: grouping_prefix +  ', '.join(x)})
    
    #Need to return this formatted for appending to the table - need to get locations of variable_code and variable name, 
    #add them in as columns in those locations and then populate them with category column nanme
    var_code_col_location = census_df.columns.get_loc('variable_code')
    var_name_col_location = census_df.columns.get_loc('variable_name')
    var_moe_col_location = census_df.columns.get_loc('MarginOfError')
    grouped_data.insert(var_moe_col_location, 'MarginOfError', '')
    #grouped_data.insert(var_code_col_location, 'variable_code','Grouped Value')
    grouped_data.insert(var_name_col_location, 'variable_name','')
    #grouped_data['variable_code'] = grouped_data['variable_code'] +  '_Grouped'
    grouped_data['variable_name'] = grouped_data[category_column]
    grouped_data['dataset']= grouping_prefix + grouped_data['dataset']
    grouped_data['variable_category']= grouping_prefix +  grouped_data['variable_category'] 
    columns_to_keep = [column for column in census_df if column not in ['OBJECTID']]
    grouped_data= grouped_data[columns_to_keep]
    return grouped_data
def sum_across_levels(df, variable_name, category_name):
    filtered_df = df.loc[(df['variable_name']==variable_name)]
    basin_summary = filtered_df.groupby([ 'dataset', 'sample_level', 'variable_name', 'variable_code', 'year_sample'], as_index=False).sum(['value'])
    county_summary = filtered_df.groupby(['dataset', 'sample_level', 'variable_name', 'variable_code', 'year_sample', 'county_name'], as_index=False).sum(['value'])
    north_south_summary = filtered_df.groupby(['dataset', 'sample_level', 'variable_name', 'variable_code', 'year_sample', 'north_south'], as_index=False).sum(['value'])
    state_summary = filtered_df.groupby(['dataset', 'sample_level', 'variable_name', 'variable_code', 'year_sample', 'state_name'], as_index=False).sum(['value'])
    #basin_summary.rename(columns = {'variable_code': 'Code', 'year_sample': 'Year'})
    basin_summary['Geography'] = 'Basin'
    county_summary['Geography'] = county_summary['county_name'] 
    north_south_summary['Geography'] = north_south_summary['north_south']
    state_summary['Geography'] = state_summary['state_name']
    columns_to_keep = ['variable_code','variable_name', 'value', 'Geography', 'year_sample', 'dataset', 'sample_level']
    basin_summary= basin_summary[columns_to_keep]
    county_summary = county_summary[columns_to_keep]
    north_south_summary = north_south_summary[columns_to_keep]
    state_summary = state_summary[columns_to_keep]
    combined_summary = pd.concat([basin_summary, county_summary, north_south_summary, state_summary], ignore_index=True)
    #if neighborhood_yn == 'Yes':
    #    neighborhood_summary = filtered_df.groupby(['dataset', 'sample_level', 'variable_name', 'variable_code', 'year_sample', 'NEIGHBORHOOD'], as_index=False).sum(['value'])
        #basin_summary.rename(columns = {'variable_code': 'Code', 'year_sample': 'Year'})
    #    neighborhood_summary['Geography'] = neighborhood_summary['NEIGHBORHOOD']
    #    combined_summary = pd.concat([combined_summary, neighborhood_summary], ignore_index=True)
    combined_summary['Category'] = category_name
    return combined_summary
def sum_multiple_variables(df, variable_list, variable_category):
    values_df=[]
    for variable in variable_list:
        summed_df = sum_across_levels(df,variable, variable_category)
        values_df = values_df.append(summed_df)
    values_df = pd.concat(values_df, ignore_index=True)
    return values_df
def median_across_levels(df, bin_column, sort_column, count_column, category_field, category,grouping_variables, cumulative_sorting_variables):
    basin_sort_columns = cumulative_sorting_variables
    basin_grouping_variables = grouping_variables
    county_sort_columns = ['county_name'] + cumulative_sorting_variables
    county_grouping_variables = grouping_variables + ['county_name']
    north_south_sort_columns = ['north_south'] + cumulative_sorting_variables
    north_south_grouping_variables = grouping_variables + ['north_south']
    basin_summary = calculate_median_value(df, bin_column, sort_column, count_column, category_field, category,grouping_variables, cumulative_sorting_variables)
    county_summary = calculate_median_value(df, bin_column, sort_column, count_column, category_field, category, county_grouping_variables, county_sort_columns)
    north_south_summary = calculate_median_value(df, bin_column, sort_column, count_column, category_field, category, north_south_grouping_variables, north_south_sort_columns)
    basin_summary['Geography'] = 'Basin'
    county_summary['Geography'] = county_summary['county_name']
    north_south_summary['Geography'] = north_south_summary['north_south']
    columns_to_keep = ['variable_code','variable_name', 'value', 'Geography', 'year_sample', 'dataset', 'sample_level', 'median_value']
    basin_summary= basin_summary[columns_to_keep]
    county_summary = county_summary[columns_to_keep]
    north_south_summary = north_south_summary[columns_to_keep]
    combined_summary = pd.concat([basin_summary, county_summary, north_south_summary], ignore_index=True)
    combined_summary['Category'] = category
    return combined_summary
