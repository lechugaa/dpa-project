import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.data_ingestion_task import DataIngestionTask


class IngestionMetadataTask(CopyToTable):

    # parameters
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=None)

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
        return DataIngestionTask(historic=self.historic, query_date=self.query_date)

    def rows(self):
        rows = [("2021-04-11", False, 100, 100.0)]
        for row in rows:
            yield row
