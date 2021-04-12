from src.utils.general import get_pickle_from_s3_to_pandas

# from datetime import datetime
# query_date = datetime(2021,4,10)

class DataCleaner:

    def __init__(self, historic=False, query_date=None):
        self.df = get_pickle_from_s3_to_pandas(historic, query_date)
        self.historic = historic
        self.query_date = query_date
        self._clean_data()

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


    def _clean_data(self):
        self.original_rows, self.original_cols = self.df.shape
        self.df.dropna()
        self.final_rows, self.final_cols = self.df.shape

    def get_clean_df(self):
        return self.df

    def get_cleaning_metadata(self):
        return [(self.original_rows,
            self.original_cols,
            self.final_rows,
            self.final_cols, self.historic, self.query_date)]

