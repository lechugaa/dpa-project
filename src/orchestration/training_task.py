import datetime
import luigi
import luigi.contrib.s3

from src.pipeline.modelling import Modelling
from src.orchestration.feature_engineering_metadata_task import FeatureEngineeringMetaTask
from src.utils.general import get_upload_path, get_file_path, get_s3_credentials
from src.utils.constants import bucket_name
from src.pipeline.ingesta_almacenamiento import get_s3_resource


class TrainingTask(luigi.Task):

    # class attributes
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return FeatureEngineeringMetaTask(historic=self.historic, query_date=self.query_date, training=True)

    def run(self):

        # obteniendo paths
        upload_path = get_upload_path(historic=self.historic, query_date=self.query_date, prefix=Modelling.prefix, training=False)
        local_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=Modelling.prefix)

        # entrenando modelos
        model_trainer = Modelling(historic=self.historic, query_date=self.query_date, save_models=True)

        # uploading file
        s3_resource = get_s3_resource()
        s3_resource.meta.client.upload_file(local_path, bucket_name, upload_path)

    def output(self):
        s3_credentials = get_s3_credentials("conf/local/credentials.yaml")
        client = luigi.contrib.s3.S3Client(
            aws_access_key_id=s3_credentials['aws_access_key_id'],
            aws_secret_access_key=s3_credentials['aws_secret_access_key']
        )

        output_path = f"s3://{bucket_name}/{get_upload_path(historic=self.historic, query_date=self.query_date, prefix=Modelling.prefix, training=False)}"
        return luigi.contrib.s3.S3Target(path=output_path, client=client)
