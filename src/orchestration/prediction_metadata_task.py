import datetime
import luigi
from luigi.contrib.postgres import CopyToTable

from src.utils.general import get_db_credentials
from src.orchestration.prediction_test_task import PredictionTestTask
from src.pipeline.prediction import Predictor


class PredictionMetaTask(CopyToTable):
    # parameters
    query_date = luigi.DateParameter(default=datetime.date.today())
    min_desired_score = luigi.FloatParameter(default=0.0)
    max_desired_score = luigi.FloatParameter(default=1.0)

    # recuperando credenciales de base de datos
    credentials = get_db_credentials('conf/local/credentials.yaml')

    # overriding atributos necesarios
    user = credentials['user']
    password = credentials['password']
    database = credentials['database']
    host = credentials['host']
    port = credentials['port']

    # nombre de tabla de meta datos
    table = 'prediction_metadata'

    # formato de tabla
    columns = [("query_date", "DATE"),
               ("prediction_date", "DATE"),
               ("model_date", "DATE"),
               ("model_s3_path", "VARCHAR(250)"),
               ("threshold", "FLOAT"),
               ("no_predictions", "INT"),
               ("no_positive_labels", "INT"),
               ("no_negative_labels", "INT")]

    def requires(self):
        return PredictionTestTask(query_date=self.query_date, min_desired_score=self.min_desired_score, max_desired_score=self.max_desired_score)

    def rows(self):
        predictor = Predictor(query_date=self.query_date)
        rows = predictor.get_metadata()
        for row in rows:
            yield row
