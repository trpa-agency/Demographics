import pandas as pd
from arcgis.features import FeatureLayer

def create_or_append_df(df, summary_df):
    if df.empty:
        df = summary_df.copy()
    else:
        df = pd.concat([df, summary_df])
    return df
def get_census_data(featureset):
    service_id = {
        'raw_data':'28',
        'summaries':'18',
        'enrollment':'32',
        'visitation':'33',
        'water_use':'34'
    }

    service_number = service_id.get(featureset) 
    service_url = 'https://maps.trpa.org/server/rest/services/Demographics/MapServer/'+service_number

    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query()
    # Convert the query result to a list of dictionaries
    feature_list = query_result.features

    # Create a pandas DataFrame from the list of dictionaries
    all_data = pd.DataFrame([feature.attributes for feature in feature_list])

    return all_data

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
    census_df = census_df.loc[census_df['variable_code'].isin(categories['variable_code'])]    
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

def calculate_sum_and_margin_of_error(group):
    sum_of_values = group['value'].sum()
    total_moe = group['MarginOfError'].apply(lambda x: x**2).sum()**0.5
    return pd.Series({'value':sum_of_values,'MarginOfError':total_moe})

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

def sum_across_levels_moe(df, variable_name, category_name):
    filtered_df = df.loc[(df['variable_name']==variable_name)]
    grouping_variables_basin = [ 'dataset', 'sample_level', 'variable_name', 'variable_code', 'year_sample']
    grouping_variables_county = grouping_variables_basin + ['county_name']
    grouping_variables_north_south = grouping_variables_basin + ['north_south']
    grouping_variables_state = grouping_variables_basin + ['state_name']
    basin_summary = filtered_df.groupby(grouping_variables_basin, as_index=False).apply(calculate_sum_and_margin_of_error)
    county_summary = filtered_df.groupby(grouping_variables_county, as_index=False).apply(calculate_sum_and_margin_of_error)
    north_south_summary = filtered_df.groupby(grouping_variables_north_south, as_index=False).apply(calculate_sum_and_margin_of_error)
    state_summary = filtered_df.groupby(grouping_variables_state, as_index=False).apply(calculate_sum_and_margin_of_error)
    #basin_summary.rename(columns = {'variable_code': 'Code', 'year_sample': 'Year'})
    basin_summary['Geography'] = 'Basin'
    county_summary['Geography'] = county_summary['county_name'] 
    north_south_summary['Geography'] = north_south_summary['north_south']
    state_summary['Geography'] = state_summary['state_name']
    columns_to_keep = ['variable_code','variable_name', 'value', 'MarginOfError', 'Geography', 'year_sample', 'dataset', 'sample_level']
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
    df_values=pd.DataFrame()
    for variable in variable_list:
        summed_df = sum_across_levels(df,variable, variable_category)
        df_values = create_or_append_df(df_values, summed_df)
    return df_values

def categorize_values_yearly (df, year, grouping_csv, category_column, grouping_prefix):
    variables = pd.read_csv(grouping_csv)
    df_filtered = df.loc[(df['variable_code'].isin(variables['variable_code']))&(df['year_sample']==year)]
    categorized_df = categorize_values(df,grouping_csv,category_column,grouping_prefix)