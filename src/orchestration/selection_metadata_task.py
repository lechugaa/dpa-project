import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.selection_test_task import SelectionTestTask
from src.pipeline.modelling import ModelSelector


class SelectionMetaTask(CopyToTable):

    # parameters
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=datetime.date.today())
    desired_models = luigi.IntParameter(default=2)
    fpr_restriction = luigi.FloatParameter(default=1.00)
    desired_classes = luigi.ListParameter(default=[0, 1])

    # recuperando credenciales de base de datos
    credentials = get_db_credentials('conf/local/credentials.yaml')

    # overriding atributos necesarios
    user = credentials['user']
    password = credentials['password']
    database = credentials['database']
    host = credentials['host']
    port = credentials['port']

    # nombre de tabla de meta datos
    table = 'selection_metadata'

    # formato de tabla
    columns = [("historic", "BOOLEAN"),
               ("query_date", "DATE"),
               ("best_model", "VARCHAR(250)"),
               ("possible_models", "INT"),
               ("cutting_threshold", "FLOAT"),
               ("fpr_restriction", "FLOAT"),
               ("best_auc", "FLOAT")]

    def requires(self):
        return SelectionTestTask(historic=self.historic, query_date=self.query_date,
                                 desired_models=self.desired_models, fpr_restriction=self.fpr_restriction,
                                 desired_classes=self.desired_classes)

    def rows(self):
        model_selector = ModelSelector(historic=self.historic, query_date=self.query_date, fpr_restriction=self.fpr_restriction, save_model=False)
        rows = model_selector.get_selection_metadata()
        for row in rows:
            yield row
