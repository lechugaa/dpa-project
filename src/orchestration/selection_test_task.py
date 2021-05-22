import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.selection_task import SelectionTask
from src.tests.selection_tests import ModelSelectionTester


class SelectionTestTask(CopyToTable):

    # parameters
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=datetime.date.today())
    desired_models = luigi.IntParameter(default=2)
    fpr_restriction = luigi.FloatParameter(default=0.20)
    desired_classes = luigi.ListParameter(default=[0, 1])

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
        return SelectionTask(historic=self.historic, query_date=self.query_date, desired_models=self.desired_models, fpr_restriction=self.fpr_restriction)

    def rows(self):
        tester = ModelSelectionTester(historic=self.historic, query_date=self.query_date, desired_classes=self.desired_classes)
        results = tester()
        if len(results.failures) > 0:
            for failure in results.failures:
                print(failure)
            raise Exception("Selection tests failed...")

        rows = [(str(datetime.date.today()), "selection-test")]
        for row in rows:
            yield row
