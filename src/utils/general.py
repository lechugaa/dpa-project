import datetime
import yaml


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


def get_api_token(credentials_file):
    """
    Lee el archivo 'credentials.yaml' que se encuentra en 'conf/local'
    :param credentials_file: ruta a archivo yaml con credenciales
    :return: token de Chicago Food Inspections API
   """
    credentials = read_yaml(credentials_file)
    api_token = credentials['food_inspections']['api_token']

    return api_token


def get_upload_path(historic=False):
    """
    Regresa un string con la ruta necesaria para almacenamiento en el bucket de S3. El parámetro 'historic'
    determina si se regresa la ruta de ingesta histórica o de ingesta continua. En ambos casos la fecha
    agregada a la ruta es la del momento de ejecución de la función. El formato de la fecha usado es %Y-%m-%d

    Ejemplos:
        historic=True -> return "ingestion/initial/historic-inspections-2020-02-02.pkl"
        historic=False -> return "ingestion/consecutive/consecutive-inspections-2020-02-02.pkl"
    :param historic: boolean
    :return: string
    """
    today_string = datetime.datetime.now().strftime("%Y-%m-%d")

    if historic:
        return f"ingestion/initial/historic-inspections-{today_string}.pkl"

    return f"ingestion/consecutive/consecutive-inspections-{today_string}.pkl"
