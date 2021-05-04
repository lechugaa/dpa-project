import datetime
import luigi
import luigi.contrib.s3

from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.pipeline.bias_fairness import MrFairnes
from src.orchestration.selection_metadata_task import SelectionMetaTask
from src.utils.general import get_file_path, get_upload_path, get_s3_credentials
from src.utils.constants import bucket_name


class AequitasTask(luigi.Task):
    historic = luigi.BoolParameter(default=False)
    # confuso: en una parte requiere que sea True y en otra False
    training = luigi.BoolParameter(default=True)
    query_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return SelectionMetaTask(historic=self.historic, query_date=self.query_date)

    def run(self):
        file_path = get_file_path(historic=self.historic, query_date=self.query_date,
                                  prefix=MrFairness.prefix, training=self.training)
        upload_path = get_upload_path(
            historic=self.historic, query_date=self.query_date, prefix=MrFairness.prefix, training=self.training)

        # execute process
        fair = MrFairness(historic=self.historic, query_date=self.query_date,
                          training=self.training, save=True)

        s3_resource = get_s3_resource()
        s3_resource.meta.client.upload_file(
            file_path, bucket_name, upload_path)

    def output(self):
        s3_credentials = get_s3_credentials("conf/local/credentials.yaml")
        client = luigi.contrib.s3.S3Client(
            aws_access_key_id=s3_credentials['aws_access_key_id'],
            aws_secret_access_key=s3_credentials['aws_secret_access_key']
        )

        upload_path = get_upload_path(
            historic=self.historic, query_date=self.query_date, prefix=MrFairness.prefix, training=self.training)
        output_path = f"s3://{bucket_name}/{upload_path}"

        return luigi.contrib.s3.S3Target(path=output_path, client=client)
