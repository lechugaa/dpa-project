import datetime
import luigi
import luigi.contrib.s3

from src.pipeline.modelling import ModelSelector
from src.orchestration.training_metadata_task import TrainingMetaTask
from src.utils.general import get_upload_path, get_file_path, get_s3_credentials
from src.utils.constants import bucket_name
from src.pipeline.ingesta_almacenamiento import get_s3_resource


class SelectionTask(luigi.Task):

    # class attributes
    historic = luigi.BoolParameter(default=False)
    query_date = luigi.DateParameter(default=datetime.date.today())
    desired_models = luigi.IntParameter(default=2)
    fpr_restriction = luigi.FloatParameter(default=0.20)

    def requires(self):
        return TrainingMetaTask(historic=self.historic, query_date=self.query_date, desired_models=self.desired_models)

    def run(self):

        # obteniendo paths
        upload_path = get_upload_path(historic=self.historic, query_date=self.query_date, prefix=ModelSelector.prefix, training=False)
        local_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=ModelSelector.prefix, training=False)

        # entrenando modelos
        model_selector = ModelSelector(historic=self.historic, query_date=self.query_date, fpr_restriction=self.fpr_restriction, save_model=True)

        # uploading file
        s3_resource = get_s3_resource()
        s3_resource.meta.client.upload_file(local_path, bucket_name, upload_path)

    def output(self):
        s3_credentials = get_s3_credentials("conf/local/credentials.yaml")
        client = luigi.contrib.s3.S3Client(
            aws_access_key_id=s3_credentials['aws_access_key_id'],
            aws_secret_access_key=s3_credentials['aws_secret_access_key']
        )

        output_path = f"s3://{bucket_name}/{get_upload_path(historic=self.historic, query_date=self.query_date, prefix=ModelSelector.prefix, training=False)}"
        return luigi.contrib.s3.S3Target(path=output_path, client=client)
