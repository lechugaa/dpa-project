# python modules
from datetime import datetime
import pandas as pd
import pickle

# local scripts
from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.utils.general import  get_upload_path, load_from_pickle
from src.utils.constants import bucket_name


def get_pickle_from_s3_to_pandas(historic=False, query_date=None):
    """
    Una función para simplificar el proceso de cargar un pickle
    desde S3 a pandas.
    : param pickle-path: el nombre del pickle incluyendo folder ingesta/consecutiva/ejemplo.pkl
    : returns df: pandas dataframe con la información lista. 
    """
    # actualizar la fecha en caso de que no nos las den
    if query_date is None:
        query_date = datetime.now().strftime("%Y-%m-%d")
    else:
        date_string = query_date.strftime("%Y-%m-%d")

    client = get_s3_resource()
    pickle_path = get_upload_path(False, query_date)
    obj = client.Object(bucket_name, pickle_path).get()['Body'].read()
    df = pd.DataFrame(pickle.loads(obj))
    print(f"Successfully loaded Dataframe from {pickle_path}")

    return df


