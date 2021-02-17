from datetime import time

def get_client(token):
    """
    Regresa un cliente que se puede conectar a la API de inspecciones de establecimiento
    dándole un token previamente generado.
    :param token: token de API
    :return: cliente de API
    """
    pass

def ingesta_inicial(api_client, limit):
    """
    Recibe como parámetros el cliente con el que nos podemos comunicar con la API,
    y el límite de registros que queremos obtener al llamar a la API
    :param api_client: cliente de API
    :param limit: número de registros que buscamos obtener
    :return: lista de los elementos que la API regresó
    """
    pass

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

def get_s3_resource():
    """
    Utiliza la función src.utils.general.get_s3_credentials() para obtener un recurso de S3
    y lo regresa para ser utilizado para almacenamiento.
    :return: Resource de S3 para poder guardar datos en el bucket
    """
    pass

def guardar_ingesta(s3_bucket_name, path, data):
    """
    :param s3_bucket_name: Nombre del bucket de S3
    :param path: ruta en el bucket en donde se guardarán los datos
    :param data: datos ingestados en csv
    """
    pass
