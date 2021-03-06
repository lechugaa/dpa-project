import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.data_s3_upload_testing_task import DataS3UploadTestingTask
from src.pipeline.ingesta_almacenamiento import generar_metadatos_almacenamiento


class UploadMetadataTask(CopyToTable):

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
    table = 'upload_metadata'

    # formato de tabla
    columns = [("ingestion_date", "DATE"),
               ("historic", "BOOLEAN"),
               ("file_name", "VARCHAR(250)")]

    def requires(self):
        return DataS3UploadTestingTask(historic=self.historic, query_date=self.query_date)

    def rows(self):
        rows = generar_metadatos_almacenamiento(self.historic, self.query_date)
        for row in rows:
            yield row
