import boto3
import datetime
import os
import pandas as pd
import pickle


from sodapy import Socrata
from src.utils.general import get_s3_credentials, get_api_token, get_file_path, load_from_pickle, get_upload_path


def get_client():
    """
    Regresa un cliente que se puede conectar a la API de inspecciones de establecimiento
    dándole un token previamente generado.
    :return: cliente de API
    """
    token = get_api_token("conf/local/credentials.yaml")
    return Socrata("data.cityofchicago.org", token)


def ingesta_inicial(client, query_date=None, limit=300000):
    """
    Recibe como parámetros el cliente con el que nos podemos comunicar con la API,
    y el límite de registros que queremos obtener al llamar a la API
    :param client: cliente de API
    :param query_date: fecha tipo datetime a partir de la cual se quieren obtener nuevas observaciones
    :param limit: número de registros que buscamos obtener
    :return: lista de los elementos que la API regresó
    """
    if query_date is None:
        results = client.get("4ijn-s7e5", limit=limit)
        return results

    query_date = query_date.strftime("%Y-%m-%d")
    # el operador < se usa en lugar de <= ya que el comportamiento esperado es que se corra a las 4am esta
    # tarea con la fecha del día lunes en que se ejecuta y en ese horario todavía no se tienen los datos del mismo
    # lunes. De esta manera si se pone como fecha un lunes cualquiera en el pasado, el resultado será el mismo que
    # ejecutarlo ese lunes en cuestión a las 4am
    query = f"inspection_date < '{query_date}'"
    results = client.get("4ijn-s7e5", where=query, limit=limit)

    return results


def ingesta_consecutiva(client, query_date=None, limit=1000):
    """
    Recibe como parámetros el cliente con el que nos podemos comunicar con la API,
    la fecha de la que se quieren obtener nuevos datos al llamar a la API
    y el límite de registros para obtener de regreso.
    :param client: cliente de API
    :param query_date: fecha tipo datetime a partir de la cual se quieren obtener nuevas observaciones
    Si no se otorga este parámetro, se genera automáticamente usando la fecha de 7 días atrás con respecto
    al día que se ejecuta la función.
    :param limit: número de registros que buscamos obtener
    :return: lista de los elementos que la API regresó
    """

    # en caso de que no se alimente un query_date, se genera con delta de días
    if query_date is None:
        query_date = datetime.datetime.now()

    # Por ahora necesitamos un delta de 7 días ya que los lunes a las 4am ya se tienen todos los datos anteriores
    # a dicho lunes. Esto porque los fines de semana no se hacen inspecciones y el sábado anterior se actualizan los
    # datos del viernes pasado
    delta = datetime.timedelta(days=7)
    query_date = (query_date - delta)

    # obteniendo en formato de texto la fecha del query
    query_date = query_date.strftime("%Y-%m-%d")
    query = f"inspection_date >= '{query_date}'"
    results = client.get("4ijn-s7e5", where=query, limit=limit)

    return results


def get_s3_resource():
    """
    Lee las credenciales de AWS de conf/local/credentials.yaml para obtener un recurso de S3
    y lo regresa para ser utilizado para almacenamiento.
    :return: Resource de S3 para poder guardar datos en el bucket
    """
    # Initiate session in AWS
    s3_credentials = get_s3_credentials("conf/local/credentials.yaml")
    session = boto3.Session(
        aws_access_key_id=s3_credentials['aws_access_key_id'],
        aws_secret_access_key=s3_credentials['aws_secret_access_key'])
    
    return session.resource('s3')


def guardar_ingesta(bucket_name, bucket_path, data, s3_resource):
    """
    :param bucket_name: Nombre del bucket de S3
    :param bucket_path: ruta en el bucket en donde se guardarán los datos
    :param data: datos ingestados por API en json
    :param s3_resource: recurso de s3
    """

    s3_resource.Object(bucket_name, bucket_path).put(Body=pickle.dumps(data))
    # s3_resource.meta.client.upload_file("data_ingesta.pkl", s3_bucket_name, path)


def generar_metadatos_ingesta(historic=False, query_date=None):
    """Función para generar metadata para el ingestion task. 
    Regresa la fecha, si fue histórica o no, el número de obs. y el espacio
    en memoria que ocupa el df.
    """
    path = get_file_path(historic, query_date)
    json_response = load_from_pickle(path)
    ingestion_df = pd.DataFrame.from_dict(json_response)

    if query_date is None:
        ingestion_date = datetime.datetime.now().strftime("%Y-%m-%d")
    else:
        ingestion_date = query_date.strftime("%Y-%m-%d")

    num_obs = ingestion_df.shape[0]
    data_size = os.path.getsize(path)

    return [(str(ingestion_date), historic, num_obs, data_size)]


def generar_metadatos_almacenamiento(historic=False, query_date=None):
    """
    Función para generar metadata para el task de almacenamiento.
    Regresa la fecha, si fue histórico o no y la ruta del archivo en S3.
    """
    if query_date is None:
        upload_date = datetime.datetime.now().strftime("%Y-%m-%d")
    else:
        upload_date = query_date.strftime("%Y-%m-%d")

    upload_path = get_upload_path(historic, query_date)

    return [(upload_date, historic, upload_path)]
