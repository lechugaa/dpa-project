import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.data_s3_upload_task import DataS3UploadTask
from src.tests.upload_tests import UploadTest


class DataS3UploadTestingTask(CopyToTable):

    # parameters
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=datetime.date.today())

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
        return DataS3UploadTask(historic=self.historic, query_date=self.query_date)

    def rows(self):
        tester = UploadTest(historic=self.historic, query_date=self.query_date)
        results = tester()
        if len(results.failures) > 0:
            for failure in results.failures:
                print(failure)
            raise Exception("Upload tests failed...")

        rows = [(str(datetime.date.today()), "upload-test")]
        for row in rows:
            yield row
