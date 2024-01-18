import pandas as pd
import numpy as np
import requests
import pyodbc
import arcpy
from arcgis.features import FeatureLayer
import psutil
import logging
import sys


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

def fixup_acs_data(df_total, tahoe_geometry, dataset, census_geom_year):
    df_total['sample_level']='tract'
    df_total['dataset']= dataset
    df_total['census_geom_year'] = census_geom_year
    df_total['GEO_ID'] = df_total['GEO_ID'].str.split('US').str[1]
    df_total['TRPAID'] = df_total['GEO_ID']+df_total['census_geom_year'].astype(str)

    print("Got to ACS")
    
    df_total = df_total[df_total['TRPAID'].isin(tahoe_geometry['TRPAID'])]
    print("got to merge")
    df_total =  pd.merge(df_total, tahoe_geometry[['TRPAID', 'NEIGHBORHOOD']], on='TRPAID', how= 'left')
    print("Completed merge")
    return df_total
def fixup_acs_data_bg(df_total, tahoe_geometry, dataset, census_geom_year):
    df_total['sample_level']='block group'
    df_total['dataset']= dataset
    df_total['census_geom_year'] = census_geom_year
    df_total['GEO_ID'] = df_total['GEO_ID'].str.split('US').str[1]
    df_total['TRPAID'] = df_total['GEO_ID']+df_total['census_geom_year'].astype(str)

    print("Got to ACS")
    
    df_total = df_total[df_total['TRPAID'].isin(tahoe_geometry['TRPAID'])]
    print("got to merge")
    df_total =  pd.merge(df_total, tahoe_geometry[['TRPAID', 'NEIGHBORHOOD']], on='TRPAID', how= 'left')
    print("Completed merge")
    return df_total
df_total = pd.read_csv('acs_2022_bg.csv')
clean_data = fixup_acs_data_bg(df_total,tahoe_geometry,'acs/acs5',2020)
clean_data.to_excel('cleaned_data_bg.xlsx')