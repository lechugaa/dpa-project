import boto3
import datetime
import os
import pandas as pd
import pickle
import yaml

from src.utils.constants import bucket_name


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


def read_yaml(credentials_file):
    """
    Lee un archivo yaml
    :param credentials_file: ruta a archivo de lectura
    :return: objeto
    """
    try:
        with open(credentials_file, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError('Could not load the file')

    return config


def get_s3_credentials(credentials_file):
    """
    Lee el archivo 'credentials.yaml' que se encuentra en 'conf/local'
    :param credentials_file: ruta a archivo yaml con credenciales
    :return: credenciales de aws
    """
    credentials = read_yaml(credentials_file)
    s3_credentials = credentials['s3']

    return s3_credentials


def get_db_credentials(credentials_file):
    """
    Lee el archivo 'credentials.yaml' que se encuentra en 'conf/local'
    :param credentials_file: ruta a archivo yaml con credenciales
    :return: credenciales de acceso a base de datos en AWS
    """
    credentials = read_yaml(credentials_file)
    return credentials['data_base']


def get_api_token(credentials_file):
    """
    Lee el archivo 'credentials.yaml' que se encuentra en 'conf/local'
    :param credentials_file: ruta a archivo yaml con credenciales
    :return: token de Chicago Food Inspections API
   """
    credentials = read_yaml(credentials_file)
    api_token = credentials['food_inspections']['api_token']

    return api_token


def get_file_path(historic=False, query_date=None, prefix="ingestion"):
    """
    Regresa un string con la ruta necesaria para almacenamiento local. El parámetro 'historic'
    determina si se regresa la ruta de ingesta histórica o de ingesta continua. En ambos casos la fecha
    agregada a la ruta es la del momento de ejecución de la función. El formato de la fecha usado es %Y-%m-%d

    Ejemplos:
        historic=True -> return "<project_root>/temp/historic-inspections-2020-02-02.pkl"
        historic=False -> return "<project_root>/temp/consecutive-inspections-2020-02-02.pkl"
    :param historic: boolean
    :param query_date: fecha (datetime) relacionada a la obtención de los datos
    :param prefix: prefijo de archivo local en donde se almacenará
    :return: string
    """
    if query_date is None:
        date_string = datetime.datetime.now().strftime("%Y-%m-%d")
    else:
        date_string = query_date.strftime("%Y-%m-%d")

    root_path = os.getcwd()

    if historic:
        return f"{root_path}/temp/{prefix}-historic-inspections-{date_string}.pkl"

    return f"{root_path}/temp/{prefix}-consecutive-inspections-{date_string}.pkl"


# def get_file_path_(historic=False, query_date=None, prefix=''):
#     """
#     Regresa un string con la ruta necesaria para almacenamiento local. El parámetro 'historic'
#     determina si se regresa la ruta de ingesta histórica o de ingesta continua. En ambos casos la fecha
#     agregada a la ruta es la del momento de ejecución de la función. El formato de la fecha usado es %Y-%m-%d
#     Ejemplos:
#         historic=True -> return "<project_root>/temp/historic-inspections-2020-02-02.pkl"
#         historic=False -> return "<project_root>/temp/consecutive-inspections-2020-02-02.pkl"
#     :param historic: boolean
#     :param query_date: fecha (datetime) relacionada a la obtención de los datos
#     :param prefix: str
#                    relacionado con la etapa del proceso; por ejemplo 'cleaning', "feature_eng"
#     :return: string
#     """
#     if query_date is None:
#         date_string = datetime.datetime.now().strftime("%Y-%m-%d")
#     else:
#         date_string = query_date.strftime("%Y-%m-%d")
#
#     root_path = os.getcwd()
#
#     if historic:
#         return f"{root_path}/temp/{prefix}-historic-inspections-{date_string}.pkl"
#
#     return f"{root_path}/temp/{prefix}-consecutive-inspections-{date_string}.pkl"


def get_upload_path(historic=False, query_date=None, prefix="ingestion"):
    """
    Regresa un string con la ruta necesaria para almacenamiento en el bucket de S3. El parámetro 'historic'
    determina si se regresa la ruta de ingesta histórica o de ingesta continua. En ambos casos la fecha
    agregada a la ruta es la del momento de ejecución de la función. El formato de la fecha usado es %Y-%m-%d

    Ejemplos:
        historic=True -> return "ingestion/initial/historic-inspections-2020-02-02.pkl"
        historic=False -> return "ingestion/consecutive/consecutive-inspections-2020-02-02.pkl"
    :param historic: boolean
    :param query_date: fecha (datetime) relacionada a la obtención de los datos
    :param prefix: comienzo de la ruta a obtener en S3
    :return: string
    """
    if query_date is None:
        date_string = datetime.datetime.now().strftime("%Y-%m-%d")
    else:
        date_string = query_date.strftime("%Y-%m-%d")

    if historic:
        return f"{prefix}/initial/historic-inspections-{date_string}.pkl"

    return f"{prefix}/consecutive/consecutive-inspections-{date_string}.pkl"


# def get_upload_path_(historic=False, query_date=None, prefix=''):
#     """
#     Regresa un string con la ruta necesaria para almacenamiento en el bucket de S3. El parámetro 'historic'
#     determina si se regresa la ruta de ingesta histórica o de ingesta continua. En ambos casos la fecha
#     agregada a la ruta es la del momento de ejecución de la función. El formato de la fecha usado es %Y-%m-%d
#     Ejemplos:
#         historic=True -> return "ingestion/initial/historic-inspections-2020-02-02.pkl"
#         historic=False -> return "ingestion/consecutive/consecutive-inspections-2020-02-02.pkl"
#     :param historic: boolean
#     :param query_date: fecha (datetime) relacionada a la obtención de los datos
#     :param prefix: str
#                   relacionado con la etapa del proceso (cleaning, fe, etc.)
#     :return: string
#     """
#     if query_date is None:
#         date_string = datetime.datetime.now().strftime("%Y-%m-%d")
#     else:
#         date_string = query_date.strftime("%Y-%m-%d")
#
#     if historic:
#         return f"preprocessing/initial/{prefix}-historic-inspections-{date_string}.pkl"
#
#     return f"preprocessing/consecutive/{prefix}-consecutive-inspections-{date_string}.pkl"


def load_from_pickle(path):
    """Loads and returns an object from a pickle file in path
    Parameters:
    path (string): Path where the pickle file resides
    Returns:
    object: Object in pickle file
    """
    infile = open(path, 'rb')
    data = pickle.load(infile)
    infile.close()
    return data


def save_to_pickle(obj, path):
    """Saves an object as a pickle file in path
    Parameters:
    obj: Object to store in pickle file
    path (string): Path where the pickle file should reside
    """
    outfile = open(path, 'wb')
    pickle.dump(obj, outfile)
    outfile.close()


def get_pickle_from_s3_to_pandas(historic=False, query_date=None):
    """
    Una función para simplificar el proceso de cargar un pickle
    desde S3 a pandas.
    :param historic: boolean indicating whether process is historic or not
    :param query_date: datetime of query
    :returns df: pandas dataframe dataframe con la info del task anterior de Luigi.
    """

    # obtener fecha de hoy en caso de que no nos las den
    if query_date is None:
        query_date = datetime.datetime.now()

    client = get_s3_resource()
    pickle_path = get_upload_path(historic, query_date)
    obj = client.Object(bucket_name, pickle_path).get()['Body'].read()
    df = pd.DataFrame(pickle.loads(obj))
    print(f"Successfully loaded Dataframe from {pickle_path}")

    return df
