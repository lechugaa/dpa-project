import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.ingestion_testing_task import IngestionTestingTask
from src.pipeline.ingesta_almacenamiento import generar_metadatos_ingesta


class IngestionMetadataTask(CopyToTable):

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
    table = 'test_ingestion_metadata'

    # formato de tabla
    columns = [("ingestion_date", "DATE"),
               ("historic", "BOOLEAN"),
               ("num_obs", "INTEGER"),
               ("data_size", "FLOAT")]

    def requires(self):
        return IngestionTestingTask(historic=self.historic, query_date=self.query_date)

    def rows(self):
        rows = generar_metadatos_ingesta(self.historic, self.query_date)
        for row in rows:
            yield row
