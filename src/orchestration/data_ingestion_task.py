import luigi
import luigi.contrib.s3
import pickle

from src.pipeline.ingesta_almacenamiento import get_client, ingesta_consecutiva, ingesta_inicial
from src.utils.general import get_file_path


class DataIngestionTask(luigi.Task):

    # parameters
    historic = luigi.BoolParameter(default=False)

    def run(self):

        # getting client for Chicago Food Inspections API
        client = get_client()

        # getting data for historic or continuous ingestion (ternary operator)
        data = ingesta_inicial(client) if self.historic else ingesta_consecutiva(client)

        # writing pickle file
        output_file = open(self.output().path, 'wb')
        pickle.dump(data, output_file)
        output_file.close()

    def output(self):
        file_path = get_file_path(historic=self.historic)
        return luigi.local_target.LocalTarget(file_path)
