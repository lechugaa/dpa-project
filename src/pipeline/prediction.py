import datetime
import os
import pickle
import numpy as np
import pandas as pd


from src.utils.general import get_file_path, load_from_pickle, get_s3_resource, get_model_from_s3
from src.utils.constants import bucket_name, models_prefix
from src.pipeline.limpieza_feature_eng import DataEngineer


class Predictor:

    # static variables
    prefix = 'predictions'

    def __init__(self, query_date):
        self.query_date = query_date
        self.prefix = Predictor.prefix
        self._setup()
        self._make_predictions()
        self._generate_df()

    def _load_features(self):
        fe_pickle = get_file_path(historic=False, query_date=self.query_date, prefix=DataEngineer.prefix, training=False)
        feature_eng_dict = load_from_pickle(fe_pickle)
        self.features = feature_eng_dict['X_consec']

    def _get_last_model_path(self):
        s3 = get_s3_resource()
        files = s3.Bucket(name=bucket_name).objects.filter(Prefix=models_prefix)

        date_fmt = "%Y-%m-%d"
        paths = [file.key for file in files]
        dates = [datetime.datetime.strptime(path.split('.')[0][-10:], date_fmt) for path in paths]
        self.model_date = max(dates)
        model_date_str = self.model_date.strftime(date_fmt)
        self.model_path = f"{models_prefix}{model_date_str}.pkl"

    def _load_model(self):
        self.model = get_model_from_s3(self.model_path)

    def _get_threshold(self):
        root_path = os.getcwd()
        path = f"{root_path}/temp/cutting_info.pkl"
        self.cutting_threshold = load_from_pickle(path)['cutting_threshold']

    def _get_original_data(self):
        self.identifiers_df = self.features[['inspection_id', 'license_']]
        self.features.drop(['inspection_id', 'license_'], axis='columns', inplace=True)

    def _setup(self):
        self._load_features()
        self._get_last_model_path()
        self._load_model()
        self._get_threshold()
        self._get_original_data()

    def _make_predictions(self):
        self.scores = self.model.predict_proba(self.features)[:, 1]
        self.labels = np.array([1 if score >= self.cutting_threshold else 0 for score in self.scores])

    def _generate_df(self):
        self.df = pd.DataFrame({
            'inspection_id': self.identifiers_df.inspection_id,
            'license_no': self.identifiers_df.license_,
            'score': self.scores,
            'labels': self.labels
        })
        self.df['threshold'] = self.cutting_threshold
        self.df['prediction_date'] = datetime.datetime.now().strftime("%Y-%m-%d")

    def save_df(self):
        local_path = get_file_path(historic=False, query_date=self.query_date, prefix=self.prefix)
        pickle.dump(self.df, open(local_path, 'wb'))
        print(f"Successfully saved temp file as pickle in: {local_path}")

    def get_metadata(self):
        predictions = self.labels.shape[0]
        positive_labels = len([label for label in self.labels if label == 1])
        negative_labels = predictions - positive_labels
        prediction_date = datetime.datetime.now().strftime("%Y-%m-%d")

        return [(self.query_date, prediction_date,
                 self.model_date, self.model_path, self.cutting_threshold,
                 predictions, positive_labels, negative_labels)]
