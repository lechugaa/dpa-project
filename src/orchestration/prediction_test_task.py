import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.prediction_task import PredictionTask
from src.tests.prediction_tests import PredictionTester


class PredictionTestTask(CopyToTable):

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

    # nombre de tabla de metadatos
    table = 'unittests'

    # formato de tabla
    columns = [("test_date", "DATE"),
               ("test_name", "VARCHAR(250)")]

    def requires(self):
        return PredictionTask(query_date=self.query_date)

    def rows(self):
        tester = PredictionTester(query_date=self.query_date,
                                  min_desired_score=self.min_desired_score,
                                  max_desired_score=self.max_desired_score)
        results = tester()
        if len(results.failures) > 0:
            for failure in results.failures:
                print(failure)
            raise Exception("Prediction tests failed...")

        rows = [(str(datetime.date.today()), "prediction-test")]
        for row in rows:
            yield row
