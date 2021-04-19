import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.clean_data_task import CleanDataTask
from src.pipeline.limpieza_feature_eng import DataCleaner


class CleanDataMetaTask(CopyToTable):

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

    # nombre de tabla de meta datos
    table = 'test_clean_metadata'

    # formato de tabla
    columns = [("original_rows", "INT"),
               ("original_cols", "INT"),
               ("final_rows", "INT"),
               ("final_cols", "INT"),
               ("historic", "BOOLEAN"),
               ("ingestion_date", "DATE")]

    def requires(self):
        return CleanDataTask(historic=self.historic, query_date=self.query_date)

    def rows(self):
        d = DataCleaner(self.historic, self.query_date)
        d.clean_data()
        rows = d.get_cleaning_metadata()
        for row in rows:
            yield row
