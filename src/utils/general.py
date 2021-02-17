import yaml


def read_yaml_file(yaml_file):
    """
    Lee un archivo yaml
    :param yaml_file: ruta a archivo de lectura
    :return: objeto
    """
    try:
        with open(yaml_file, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError('Could not load the file')

    return config


def get_s3_credentials(credentials_path):
    """
    Lee el archivo 'credentials.yaml' que se encuentra en 'conf/local'
    :return: credenciales de aws
    """
    credentials = read_yaml_file(credentials_path)
    s3_credentials = credentials['s3']

    return s3_credentials
