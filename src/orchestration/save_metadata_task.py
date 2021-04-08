import luigi
from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials


class SaveMetadataTask(CopyToTable):
    x = luigi.IntParameter()

    # recuperando credenciales de base de datos
    credentials = get_db_credentials('conf/local/credentials.yaml')

    # overriding atributos necesarios
    user = credentials['user']
    password = credentials['password']
    database = credentials['database']
    host = credentials['host']
    port = credentials['port']

    # nombre de tabla de metadatos
    table = 'metatest'

    # formato de tabla
    columns = [("col_1", "VARCHAR"),
               ("col_2", "VARCHAR")]

    def rows(self):
        z1 = str(self.x)
        z2 = str(self.x + self.x)
        rows = [("test 1", z1), ("test 2", z2)]
        for row in rows:
            yield row
