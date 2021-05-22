import datetime
import luigi
import luigi.contrib.s3

from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.pipeline.prediction import Predictor
from src.orchestration.feature_engineering_metadata_task import FeatureEngineeringMetaTask
from src.utils.general import get_file_path, get_upload_path, get_s3_credentials
from src.utils.constants import bucket_name


class PredictionTask(luigi.Task):
    query_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return FeatureEngineeringMetaTask(query_date=self.query_date)

    def run(self):
        file_path = get_file_path(historic=False, query_date=self.query_date, prefix=Predictor.prefix, training=False)
        upload_path = get_upload_path(historic=False, query_date=self.query_date, prefix=Predictor.prefix, training=False)

        # execute process
        predictor = Predictor(query_date=self.query_date)
        predictor.save_df()

        s3_resource = get_s3_resource()
        s3_resource.meta.client.upload_file(file_path, bucket_name, upload_path)

    def output(self):
        s3_credentials = get_s3_credentials("conf/local/credentials.yaml")
        client = luigi.contrib.s3.S3Client(
            aws_access_key_id=s3_credentials['aws_access_key_id'],
            aws_secret_access_key=s3_credentials['aws_secret_access_key']
        )

        upload_path = get_upload_path(historic=False, query_date=self.query_date, prefix=Predictor.prefix, training=False)
        output_path = f"s3://{bucket_name}/{upload_path}"

        return luigi.contrib.s3.S3Target(path=output_path, client=client)
