import datetime
import luigi
from luigi.contrib.postgres import CopyToTable


from src.utils.general import get_db_credentials, get_file_path, load_from_pickle
from src.orchestration.prediction_metadata_task import PredictionMetaTask


class ApiStorageTask(CopyToTable):
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
    table = 'api.scores'

    # formato de tabla
    columns = [("license_no", "VARCHAR(20)"),
               ("score", "FLOAT"),
               ("labels", "INT"),
               ("threshold", "FLOAT"),
               ("prediction_date", "DATE")]

    def requires(self):
        return PredictionMetaTask(query_date=self.query_date)

    def rows(self):
        file_path = get_file_path(historic=False, query_date=self.query_date, prefix="predictions", training=False)
        df = load_from_pickle(file_path)
        df.license_no = df.license_no.astype(str)
        rows = df.to_records(index=False)
        for row in rows:
            yield row
