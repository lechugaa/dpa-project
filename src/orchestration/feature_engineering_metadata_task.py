import datetime
import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.orchestration.feature_eng_test_task import FeatureEngTestTask
from src.pipeline.limpieza_feature_eng import DataEngineer


class FeatureEngineeringMetaTask(CopyToTable):

    # parameters
    historic = luigi.BoolParameter(default=False)
    training = luigi.BoolParameter(default=False)
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
    table = 'fe_metadata'

    # formato de tabla
    columns = [("original_rows", "INT"),
               ("original_cols", "INT"),
               ("final_rows", "INT"),
               ("final_cols", "INT"),
               ("historic", "BOOLEAN"),
               ("ingestion_date", "DATE"),
               ("training", "BOOLEAN")]

    def requires(self):
        return FeatureEngTestTask(historic=self.historic, query_date=self.query_date, training=self.training)

    def rows(self):
        fe = DataEngineer(historic=self.historic, query_date=self.query_date, training=self.training)
        fe.generate_features(save_df=False, save_transformers=False)
        rows = fe.get_feature_engineering_metadata()
        for row in rows:
            yield row
