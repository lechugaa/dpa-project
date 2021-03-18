import luigi
import luigi.contrib.s3
import pickle

from src.pipeline.ingesta_almacenamiento import get_client, ingesta_consecutiva, ingesta_inicial
from src.utils.general import get_file_path


class DataIngestionTask(luigi.Task):

    # parameters
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=None)

    def run(self):

        # getting client for Chicago Food Inspections API
        client = get_client()

        # getting data for historic or continuous ingestion
        if self.historic:
            data = ingesta_inicial(client=client, query_date=self.query_date)
        else:
            data = ingesta_consecutiva(client=client, query_date=self.query_date)

        # writing pickle file
        output_file = open(self.output().path, 'wb')
        pickle.dump(data, output_file)
        output_file.close()

    def output(self):
        file_path = get_file_path(historic=self.historic, query_date=self.query_date)
        return luigi.local_target.LocalTarget(file_path)
