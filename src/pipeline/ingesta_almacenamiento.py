from datetime import time
import boto3
from sodapy import Socrata
from src.utils.general import get_s3_credentials, get_chicago_api_token
from io import StringIO
import pandas as pd

def get_client(token):
    """
    Regresa un cliente que se puede conectar a la API de inspecciones de establecimiento
    dándole un token previamente generado.
    :param token: token de API
    :return: cliente de API
    """
    return Socrata("data.cityofchicago.org", token)


def ingesta_inicial(api_client, limit = 300000):
    """
    Recibe como parámetros el cliente con el que nos podemos comunicar con la API,
    y el límite de registros que queremos obtener al llamar a la API
    :param api_client: cliente de API
    :param limit: número de registros que buscamos obtener
    :return: lista de los elementos que la API regresó
    """
    results = api_client.get("4ijn-s7e5", limit=limit)
    return pd.DataFrame.from_records(results) 

def ingesta_consecutiva(api_client, fetch_date, limit=1000):
    """
    Recibe como parámetros el cliente con el que nos podemos comunicar con la API,
    la fecha de la que se quieren obtener nuevos datos al llamar a la API
    y el límite de registros para obtener de regreso.
    :param api_client: cliente de API
    :param fetch_date: fecha de la que se quieren obtener nuevos datos
    :param limit: número de registros que buscamos obtener
    :return: lista de los elementos que la API regresó
    """
    pass


def get_s3_resource(s3_creds):
    """
    Utiliza la función src.utils.general.get_s3_credentials() para obtener un recurso de S3
    y lo regresa para ser utilizado para almacenamiento.
    :return: Resource de S3 para poder guardar datos en el bucket
    """
    #Initiate session in AWS
    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key'])
    
    return session.resource('s3')
    


def guardar_ingesta(s3_bucket_name, path, data, s3_resource):
    """
    :param s3_bucket_name: Nombre del bucket de S3
    :param path: ruta en el bucket en donde se guardarán los datos
    :param data: datos ingestados en csv
    """
    #Define buffer to avoid local download
    csv_buffer = StringIO()
    data.to_csv(csv_buffer)
    
    s3_resource.Object(s3_bucket_name, path).put(Body=csv_buffer.getvalue())
    
    return s3_resource
