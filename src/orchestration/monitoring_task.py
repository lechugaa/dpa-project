import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from scipy.stats import wasserstein_distance

from src.utils.general import get_db_credentials, get_file_path, load_from_pickle, get_s3_resource
from src.utils.constants import bucket_name, models_prefix
from src.orchestration.api_storage_task import ApiStorageTask


class MonitoringTask(CopyToTable):
    # parameters
    query_date = luigi.DateParameter(default=datetime.date.today())

    # recuperando credenciales de base de datos
    credentials = get_db_credentials('conf/local/credentials.yaml')

    # overriding atributos necesarios
    user = credentials['user']
    password = credentials['password']
    database = credentials['database']
    host = credentials['host']
    port = credentials['port']

    # nombre de tabla de meta datos
    table = 'monitoring_metrics'

    # formato de tabla
    columns = [("monitoring_date", "DATE"),
               ("model_date", "DATE"),
               ("wasserstein", "FLOAT")]

    def requires(self):
        return ApiStorageTask(query_date=self.query_date)

    def rows(self):

        # obtaining wassertein's distance
        prediction_scores_path = get_file_path(query_date=self.query_date, prefix="predictions")
        prediction_scores = load_from_pickle(prediction_scores_path).score
        model_scores = load_from_pickle("temp/model-selection-predicted-scores.pkl")[:, 1]
        wd = wasserstein_distance(model_scores, prediction_scores)

        # obtaining last model date
        s3 = get_s3_resource()
        files = s3.Bucket(name=bucket_name).objects.filter(Prefix=models_prefix)
        date_fmt = "%Y-%m-%d"
        paths = [file.key for file in files]
        dates = [datetime.datetime.strptime(path.split('.')[0][-10:], date_fmt) for path in paths]
        model_date = max(dates)

        # generating rows
        rows = [(self.query_date, model_date, wd)]
        for row in rows:
            yield row
