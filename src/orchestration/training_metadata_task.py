import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.training_test_task import TrainingTestTask
from src.pipeline.modelling import Modelling


class TrainingMetaTask(CopyToTable):

    # parameters
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=datetime.date.today())
    desired_models = luigi.IntParameter(default=2)

    # recuperando credenciales de base de datos
    credentials = get_db_credentials('conf/local/credentials.yaml')

    # overriding atributos necesarios
    user = credentials['user']
    password = credentials['password']
    database = credentials['database']
    host = credentials['host']
    port = credentials['port']

    # nombre de tabla de meta datos
    table = 'test_training_metadata'

    # formato de tabla
    columns = [("historic", "BOOLEAN"),
               ("query_date", "DATE"),
               ("no_of_models", "INT"),
               ("algorithms", "VARCHAR(100)"),
               ("training_time", "FLOAT"),
               ("split_criteria", "VARCHAR(100)")]

    def requires(self):
        return TrainingTestTask(historic=self.historic, query_date=self.query_date, desired_models=self.desired_models)

    def rows(self):
        m = Modelling(historic=self.historic, query_date=self.query_date, save_models=False)
        rows = m.get_modeling_metadata()
        for row in rows:
            yield row
