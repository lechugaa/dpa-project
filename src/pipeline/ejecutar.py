#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 12:17:46 2021

@author: mario
"""

from src.pipeline.ingesta_almacenamiento import *

#Define API de Chicago Data Portal as download client
client = get_client(get_chicago_api_token('conf/local/credentials.yaml'))                  
                  
#Download last N rows
# Convert to pandas DataFrame
results_df = ingesta_inicial(client, 15000)

#Get S3 credentials
s3_creds = get_s3_credentials('conf/local/credentials.yaml')

#Get S3 resource
s3_res = get_s3_resource(s3_creds)

#Define bucket name (Previously created)
bucket_name = 'data-product-architecture-equipo-7'


#Estoy asumiendo que dentro de guardar_ingesta() utiliza el resource creado
#anteriormente, por lo que agrego el argumento
guardar_ingesta(bucket_name, 'ingestion/initial/historic-inspections-2020-02-17.csv', results_df, s3_res)


#Nestear todo en una funcion

guardar_ingesta(s3_bucket_name = 'data-product-architecture-equipo-7',
                path = 'ingestion/initial/historic-inspections-2020-02-17.csv',
                data = ingesta_inicial(get_client(get_chicago_api_token('conf/local/credentials.yaml')), 15000),
                s3_resource = get_s3_resource(get_s3_credentials('conf/local/credentials.yaml')))
