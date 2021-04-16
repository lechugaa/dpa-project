import pickle
import re
# import pandas as pd #parche

from src.utils.general import get_pickle_from_s3_to_pandas, get_file_path_
from src.utils.general import load_from_pickle

# from datetime import datetime
# query_date = datetime(2021,4,10)


class DataCleaner:

    def __init__(self, historic=False, query_date=None):
        self.df = get_pickle_from_s3_to_pandas(historic, query_date)
        # parche
        # self.df = pd.DataFrame(pickle.load(open('temp/consecutive-inspections-2021-03-15.pkl', 'rb')))
        # end parche
        self.historic = historic
        self.query_date = query_date
        self.prefix = 'clean'

# Agregar nuevo codigo

    def _subset_cols(self):
        self.df = self.df[['inspection_id', 'facility_type', 'risk', 'zip', 'inspection_date',
                           'inspection_type', 'results', 'violations', 'latitude', 'longitude']]

    def _fill_nas(self):
        self.df['zip'] = self.df['zip'].fillna(0)
        self.df[['facility_type', 'risk', 'inspection_type', 'violations']] = self.df[[
            'facility_type', 'risk', 'inspection_type', 'violations']].fillna('na')

    def _change_data_types(self):
        self.df = self.df.astype({"zip": 'int'})
        self.df = self.df.astype({"zip": 'str'})

    def _clean_results(self):
        self.df['results'].mask(self.df['results'] !=
                                'Pass', other='Not Pass', inplace=True)

    # def _standarize_column_names(self, excluded_punctuation=".,-*¿?¡!#"):
        # self.df.columns = self.df.columns.str.lower().str.replace(" ", "_")
        # for ch in excluded_punctuation:
        #    self.df.columns = self.df.columns.str.replace(ch, "")

    def _standarize_column_strings(self, columns, excluded_punctuation=".,-*'¿?¡!()", gap_punct="\/"):
        for col in columns:
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace(" ", "_"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("á", "a"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("é", "e"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("í", "i"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("ó", "o"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("ú", "u"))
            self.df[col] = self.df[col].apply(
                lambda x: x.lower().replace("ü", "u"))

        for ch in excluded_punctuation:
            self.df[col] = self.df[col].apply(lambda x: x.replace(ch, ""))

        for ch in gap_punct:
            self.df[col] = self.df[col].apply(lambda x: x.replace(ch, "_"))

    def _clean_facility_type(self):

        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('restuarant', 'restaurant'))
        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('theatre', 'theater'))
        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('herabal', 'herbal'))
        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('day_care', 'daycare'))
        self.df['facility_type'] = self.df['facility_type'].apply(
            lambda x: x.replace('long_term', 'longterm'))

        self.df.loc[self.df['facility_type'].str.contains(
            'childern|children|1023|5_years_old', case=False, na=None), 'facility_type'] = 'childrens_service_facility'
        self.df.loc[self.df['facility_type'].str.contains(
            'conv|mart|gas_station_store', case=False, na=None), 'facility_type'] = 'convenience_store'
        self.df.loc[self.df['facility_type'].str.contains(
            'assis|longterm|nursing|supportive', case=False, na=None), 'facility_type'] = 'assisted_living'
        self.df.loc[self.df['facility_type'].str.contains(
            'herbal_life|herbalife|herbalcal', case=False, na=None), 'facility_type'] = 'herbalife'
        self.df.loc[self.df['facility_type'].str.contains(
            'after_school', case=False, na=None), 'facility_type'] = 'after_school'
        self.df.loc[self.df['facility_type'].str.contains(
            'tavern|pub|brew|wine_tasting|bar_grill|hooka', case=False, na=None), 'facility_type'] = 'bar'
        self.df.loc[self.df['facility_type'].str.contains(
            'bakery', case=False, na=None), 'facility_type'] = 'bakery'
        self.df.loc[self.df['facility_type'].str.contains(
            'mobil|truck|mfd', case=False, na=None), 'facility_type'] = 'mobile_food'
        self.df.loc[self.df['facility_type'].str.contains(
            'kitchen', case=False, na=None), 'facility_type'] = 'kitchen'
        self.df.loc[self.df['facility_type'].str.contains(
            'restaurant|rstaurant|diner', case=False, na=None), 'facility_type'] = 'restaurant'
        self.df.loc[self.df['facility_type'].str.contains(
            'retail', case=False, na=None), 'facility_type'] = 'retail'
        self.df.loc[self.df['facility_type'].str.contains(
            'roof', case=False, na=None), 'facility_type'] = 'rooftop'
        self.df.loc[self.df['facility_type'].str.contains(
            'grocery', case=False, na=None), 'facility_type'] = 'grocery_store'
        self.df.loc[self.df['facility_type'].str.contains(
            'liquor', case=False, na=None), 'facility_type'] = 'liquor'
        self.df.loc[self.df['facility_type'].str.contains(
            'popup', case=False, na=None), 'facility_type'] = 'popup_establishment'
        self.df.loc[self.df['facility_type'].str.contains(
            'school|college|shcool', case=False, na=None), 'facility_type'] = 'school'
        self.df.loc[self.df['facility_type'].str.contains(
            'daycare', case=False, na=None), 'facility_type'] = 'daycare'
        self.df.loc[self.df['facility_type'].str.contains(
            'cafeteria|coffee|cafe', case=False, na=None), 'facility_type'] = 'coffee'
        self.df.loc[self.df['facility_type'].str.contains(
            'drug_store|pharmacy', case=False, na=None), 'facility_type'] = 'drug_store'
        self.df.loc[self.df['facility_type'].str.contains(
            'gym|fitness|weight_loss|exercise', case=False, na=None), 'facility_type'] = 'gym'
        self.df.loc[self.df['facility_type'].str.contains(
            'commissary|machine|commiasary', case=False, na=None), 'facility_type'] = 'vending_machine'
        self.df.loc[self.df['facility_type'].str.contains(
            'ice_cream|paleteria|gelato', case=False, na=None), 'facility_type'] = 'ice_cream'
        self.df.loc[self.df['facility_type'].str.contains(
            'banquet', case=False, na=None), 'facility_type'] = 'banquet'
        self.df.loc[self.df['facility_type'].str.contains(
            'lounge', case=False, na=None), 'facility_type'] = 'lounge'
        self.df.loc[self.df['facility_type'].str.contains(
            'church|religious', case=False, na=None), 'facility_type'] = 'church'
        self.df.loc[self.df['facility_type'].str.contains(
            'kiosk', case=False, na=None), 'facility_type'] = 'kiosk'
        self.df.loc[self.df['facility_type'].str.contains(
            'health|rehab', case=False, na=None), 'facility_type'] = 'health'
        self.df.loc[self.df['facility_type'].str.contains(
            'event', case=False, na=None), 'facility_type'] = 'events'
        self.df.loc[self.df['facility_type'].str.contains(
            'donut|hotdog|hot_dog|popcorn|juice|tea|dessert|deli|salad|snack|candy|shake|watermelon|smoothie|food|sushi', case=False, na=None), 'facility_type'] = 'other_food'
        self.df.loc[self.df['facility_type'].str.contains(
            'poultry|butcher|slaughter|meat', case=False, na=None), 'facility_type'] = 'butcher'
        self.df.loc[self.df['facility_type'].str.contains(
            'profit', case=False, na=None), 'facility_type'] = 'non_profit'
        # self.df.loc[self.df['facility_type'].str.contains('na', case=False, na=None), 'facility_type'] = 'not_specified'

    def _clean_inspection_type(self):
        self.df.loc[self.df['inspection_type'].str.contains(
            'license', case=False, na=None), 'inspection_type'] = 'license'
        self.df.loc[self.df['inspection_type'].str.contains(
            'task_force|taskforce', case=False, na=None), 'inspection_type'] = 'task_force'
        self.df.loc[self.df['inspection_type'].str.contains(
            'canvass|canvas', case=False, na=None), 'inspection_type'] = 'canvas'
        self.df.loc[self.df['inspection_type'].str.contains(
            'complaint', case=False, na=None), 'inspection_type'] = 'complaint'
        self.df.loc[self.df['inspection_type'].str.contains(
            'food|sick', case=False, na=None), 'inspection_type'] = 'suspected_food_poisoning'

    def _crea_num_violations(self):
        self.df['num_violations'] = self.df['violations'].apply(
            lambda x: x.count(' | ') + 1 if x != 'na' else 0)

###

    def clean_data(self, save=False):
        print("Cleaning records..")
        self.original_rows, self.original_cols = self.df.shape
        # Codigo Agregado MH
        self._subset_cols()
        self._fill_nas()
        self._change_data_types()
        self._clean_results()
        self._standarize_column_strings(
            ['facility_type', 'risk', 'inspection_type'])
        self._clean_facility_type()
        self._clean_inspection_type()
        self._crea_num_violations()
        ###
        self.final_rows, self.final_cols = self.df.shape
        print("Records are clean and ready to be uploaded")
        if save:
            self._save_df()

    def _save_df(self):
        local_path = get_file_path_(
            self.historic, self.query_date, self.prefix)
        pickle.dump(self.df, open(local_path, 'wb'))
        print(f"Succesfully saved temp file as pickle in: {local_path}")

    def get_clean_df(self):
        # Codigo agregado MH
        self.clean_data()
        #
        return self.df

    def get_cleaning_metadata(self):
        return [(self.original_rows,
                 self.original_cols,
                 self.final_rows,
                 self.final_cols, self.historic, self.query_date)]


class DataEngineer:

    def __init__(self, historic=False, query_date=None):
        self.historic = historic
        self.query_date = query_date
        self.df = self._get_df()
        self.original_rows, self.original_cols = self.df.shape
        self.prefix = 'feat-eng'

    def _get_df(self):
        """Función para cargar el pickle que se guarda en local
        de la task anterior.
        """
        pickle_task_anterior = get_file_path_(
            self.historic, self.query_date, prefix='clean')
        df = load_from_pickle(pickle_task_anterior)
        df.violations = df.violations.astype('str')
        return df

    def _extract_violation_num(self, s) -> int:
        """
        Extrae el número de violación en un string.
        :param s: string
                      texto del registro
        :return: int
        """
        pattern = '_?\d+_'  # guion_bajo opcional + numeros + guion_bajo
        result = re.findall(pattern, s)
        if not result:  # no hay violaciones
            return 0
        else:  # regresa número de violación
            return int(result[0].replace('_', ''))

    def _get_violations_incurred(self, s) -> list:
        """
        Extra el total de violaciones en un registro.
        :param s: string
                      Una celda de la columna 'violations'
        :return violation_nums: list
                 lista con todas las infracciones, por ejemplo, [13,22,55]
        """
        all_violations = s.split('_~_')
        violation_nums = []
        for violation in all_violations:
            violation_nums.append(self._extract_violation_num(violation))

        return violation_nums

    def _add_extra_columns(self, df):
        """Añade columnas para one hot encoding."""
        for i in range(1, 80):
            column = 'violation_' + str(i)
            df[column] = 0

        return df

    def _add_one_hot_encoding(self, df):
        """
         Realiza todo el pipeline.

        :param df: pandas dataframe
        :return df: dataframe con el one hot encoding
         """

        # crear columnas
        df = self._add_extra_columns(df)
        df['lista_violaciones'] = df.violations.apply(self._get_violations_incurred)

        # luego llenamos df
        for i in range(len(df)):
            violaciones = df['lista_violaciones'][i]
            violaciones = [e for e in violaciones if e != '0']
            for violacion in violaciones:
                column_index = violacion + 12
                df.iloc[i, column_index] = 1

        # tiramos columnas  temporales y las que están todas en ceros
        df.drop(columns=['lista_violaciones'], inplace=True)
    
        return df
    

    def _drop_zero_cols(self, df):
        all_columns = df.columns.values
        other = [e for e in all_columns if not  e.startswith('violation_')]
        all_columns =  [e for e in all_columns if e.startswith('violation_')]
        valid_cols = []
        for column in all_columns:
            if df[column].sum() != 0:
                valid_cols.append(column)
        
        other += valid_cols
        return df[other]



    def _save_df(self):
        local_path = get_file_path_(self.historic, self.query_date, self.prefix)
        pickle.dump(self.df, open(local_path, 'wb'))
        print(f"Succesfully saved temp file as pickle in: {local_path}")

    # estos son los métodos que mandaré llamar desde Luigi

    def generate_features(self, save=False):
        """
        Genera el self.df a partir de los datos post-limpieza (los toma de S3)
        Este self.df ya debe contener todos los features que se quieren agregar
        para entrenar y predecir
        :param save: booleano que determina si se guarda localmente o no el df
        """
        self.df = self._add_one_hot_encoding(self.df)
        self.df = self._drop_zero_cols(self.df)
        self.final_rows, self.final_cols = self.df.shape
        if save:
            self._save_df()

    def get_featured_df(self):
        """
        Regresa el atributo df de la clase. Esta función debe llamarse después de
        generate_features
        :return: data frame con los features añadidos
        """
        return self.df

    def get_feature_engineering_metadata(self):
        """
        Genera los metadatos del proceso de generación de features y los regresa como una
        lista de tuplas para ser escritos en una base de datos
        :return: lista de tuplas con metadatos
        """
        return [(self.original_rows, self.original_cols, self.final_rows,
                 self.final_cols, self.historic, self.query_date)]

