import datetime
import luigi
from luigi.contrib.postgres import CopyToTable

from src.utils.general import get_db_credentials
from src.orchestration.aequitas_test_task import AequitasTestTask
from src.pipeline.bias_fairness import MrFairness

class AequitasMetaTask(CopyToTable):

    # parameters
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=datetime.date.today())
    training = luigi.BoolParameter(default=True)
    
    # recuperando credenciales de base de datos
    credentials = get_db_credentials('conf/local/credentials.yaml')

    # overriding atributos necesarios
    user = credentials['user']
    password = credentials['password']
    database = credentials['database']
    host = credentials['host']
    port = credentials['port']

    # nombre de tabla de meta datos
    table = 'aequitas_metadata'

    # formato de tabla
    columns = [("group_entities", "INT"),
                "mean_positive_rate", "FLOAT",
                "mean_ppr_disparity", "FLOAT",
                "unsupervised_fairness", "BOOLEAN",
                "supervised_fairness", "BOOLEAN",
                "overall_fairness", "BOOLEAN"]

    def requires(self):
        return AequitasTestTask(historic=self.historic, query_date=self.query_date,
                                 training=self.training)

    def rows(self):
        f= MrFairness(historic=self.historic, query_date=self.query_date,
                              training=self)
        rows = f.get_metadata()
        for row in rows:
            yield row
