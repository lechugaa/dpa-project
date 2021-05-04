import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.aequitas_task import AequitasTask
from src.tests.aequitas_tests import AequitasTester


class AequitasTestTask(CopyToTable):

    # parameters
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=datetime.date.today())
    training = luigi.BoolParameter(default=True)
    # desired_models = luigi.IntParameter(default=2)
    # fpr_restriction = luigi.FloatParameter(default=1.00)
    # desired_classes = luigi.ListParameter(default=[0, 1])

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
        return AequitasTask(historic=self.historic, query_date=self.query_date)

    def rows(self):
        tester = AequitasTester(historic=self.historic, query_date=self.query_date, training=self.training)
                 # agregar más atributos luego -> o pruebas del restaurant facility o algo 
        results = tester()
        if len(results.failures) > 0:
            for failure in results.failures:
                print(failure)
            raise Exception("Selection tests failed...")

        rows = [(str(datetime.date.today()), "bias-fairness-test")]
        for row in rows:
            yield row
