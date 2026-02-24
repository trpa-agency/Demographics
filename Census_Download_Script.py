# This script is a demo of how you would download the variables in the climate_dashboard_fill_in_3.csv file. 
import pandas as pd
import numpy as np
import requests
import pyodbc
import arcpy
import utils
from arcgis.features import FeatureLayer
from utils import *
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

tahoe_geometry = utils.get_tahoe_geometry()
#Helper function that is used to concatenate census data return
variable_list = pd.read_csv('Census_Variable_Lists\climate_dashboard_fill_in_3.csv')
session = utils.make_session()
year = '2021'
checkpoint_dir = 'Census_Download_Checkpoints'
final_output_csv = f'Census_Downloads/census_data_{year}.csv'

df = utils.census_download_wrapper_checkpointed(
    variables_df=variable_list,
    year=year,
    checkpoint_dir=checkpoint_dir,
    final_output_csv=final_output_csv,
    tahoe_geometry=tahoe_geometry,
    session=session,
    census_geom_year='2020',
    census_api_key=census_api_key)
    