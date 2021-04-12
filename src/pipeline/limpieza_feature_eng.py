import pickle

from src.utils.general import get_pickle_from_s3_to_pandas, get_file_path_

# from datetime import datetime
# query_date = datetime(2021,4,10)

class DataCleaner:

    def __init__(self, historic=False, query_date=None):
        self.df = get_pickle_from_s3_to_pandas(historic, query_date)
        self.historic = historic
        self.query_date = query_date
        self.prefix = 'clean'

    def _change_data_types(self):
        self.df['License #'] = self.df['License #'].fillna(0)
        self.df['Zip'] = self.df['Zip'].fillna(0)
        self.df = self.df.astype({"License #": 'int', "Zip": 'int'})
        self.df = self.df.astype({"License #": 'str', "Zip": 'str'})

    def _standarize_column_names(self, excluded_punctuation=".,-*¿?¡!#"):
        self.df.columns = self.df.columns.str.lower().str.replace(" ", "_")
        for ch in excluded_punctuation:
            self.df.columns = self.df.columns.str.replace(ch, "")

        self.df = self.df.rename(columns={'license_':'license'})


    def _clean_data(self, save=False):
        print("Cleaning records..")
        self.original_rows, self.original_cols = self.df.shape
        self.df.dropna()
        self.final_rows, self.final_cols = self.df.shape
        print("Records are clean and ready to be uploaded")
        if save:
            self._save_df()


    def _save_df(self):
        local_path = get_file_path_(self.historic, self.query_date, self.prefix)
        pickle.dump(self.df, open(local_path, 'wb'))
        print(f"Succesfully saved temp file as pickle in: {local_path}")

    def get_clean_df(self):
        return self.df

    def get_cleaning_metadata(self):
        return [(self.original_rows,
            self.original_cols,
            self.final_rows,
            self.final_cols, self.historic, self.query_date)]

