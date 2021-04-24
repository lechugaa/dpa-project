import pickle
import re
import pandas as pd
import datetime

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler 
from sklearn.preprocessing import OneHotEncoder

from src.utils.general import get_pickle_from_s3_to_pandas, get_file_path
from src.utils.general import load_from_pickle


class DataCleaner:

    # static variables
    prefix = 'clean'

    def __init__(self, historic=False, query_date=None):
        self.df = get_pickle_from_s3_to_pandas(historic, query_date)
        #Parche
        #self.df = pd.DataFrame(pd.read_pickle('temp/historic-inspections-2021-02-22.pkl'))
        # Parche
        self.historic = historic
        self.query_date = query_date
        self.prefix = DataCleaner.prefix

    def _subset_cols(self):
        self.df = self.df[['inspection_id', 'facility_type', 'risk', 'zip', 'inspection_date',
                           'inspection_type', 'results', 'violations', 'latitude', 'longitude']]

    def _fill_nas(self):
        self.df['zip'] = self.df['zip'].fillna(0)
        self.df[['facility_type', 'inspection_type', 'violations']] = self.df[[
            'facility_type', 'inspection_type', 'violations']].fillna('na')

    def _change_data_types(self):
        self.df = self.df.astype({
            'inspection_date': 'datetime64',
            'longitude': 'double',
            'latitude': 'double',
            'zip': 'int'})
        self.df = self.df.astype({"zip": 'str'})

    def _clean_results(self):
        self.df['results'].mask(self.df['results'] !=
                                'Pass', other='Not Pass', inplace=True)

    def _standardize_column_strings(self, columns, excluded_punctuation=".,-*'¿?¡!()", gap_punct="\/"):
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

    def _drop_risk_all(self):
        self.df = self.df[self.df['risk'] != 'all']
        
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
        
    def _sort_by_date(self):
        self.df.sort_values('inspection_date', inplace = True)
        self.df.reset_index(inplace = True,drop = True)

    def clean_data(self, save=False):
        print("Cleaning records..")
        self.original_rows, self.original_cols = self.df.shape
        # Codigo Agregado MH
        self._subset_cols()
        self._fill_nas()
        self.df.dropna(axis = 0, inplace = True)
        self._change_data_types()
        self._clean_results()
        self._standardize_column_strings(['facility_type', 'risk', 'inspection_type', 'results'])
        self._drop_risk_all()
        self._clean_facility_type()
        self._clean_inspection_type()
        self._crea_num_violations()
        self._sort_by_date()
        ###
        self.final_rows, self.final_cols = self.df.shape
        print("Records are clean and ready to be uploaded")
        if save:
            self._save_df()

    def _save_df(self):
        local_path = get_file_path(self.historic, self.query_date, self.prefix)
        pickle.dump(self.df, open(local_path, 'wb'))
        print(f"Successfully saved temp file as pickle in: {local_path}")

    def get_clean_df(self):
        self.clean_data()
        return self.df

    def get_cleaning_metadata(self):
        if self.query_date is None:
            self.query_date = datetime.datetime.now()

        return [(self.original_rows,
                 self.original_cols,
                 self.final_rows,
                 self.final_cols, self.historic, self.query_date)]


class DataEngineer:

    # static variables
    prefix = 'feature-engineering'

    def __init__(self, historic=False, query_date=None):
        self.historic = historic
        self.query_date = query_date
        self.df = self._get_df()
        # Parche
        # self.df = pd.DataFrame(pickle.load(open('temp/clean-historic-inspections-2021-02-22.pkl', 'rb')))
        # end parche
        self.original_rows, self.original_cols = self.df.shape
        self.prefix = DataEngineer.prefix

    def _get_df(self):
        """Función para cargar el pickle que se guarda en local
        de la task anterior.
        """
        pickle_task_anterior = get_file_path(
            self.historic, self.query_date, prefix='clean')
        df = load_from_pickle(pickle_task_anterior)      
        df.violations = df.violations.astype('str')
        return df

    # Código MH
    
    def _split_data(self):
        y = self.df['results'] 
        X = self.df.drop(columns = 'results')

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(X, y, shuffle = False)
        
    def _get_date_features(self):
        
        self.X_train['dow'] = self.X_train['inspection_date'].dt.day_name().str.lower()
        self.X_train['month'] = self.X_train['inspection_date'].dt.month
        self.X_train = self.X_train.astype({"month": 'str'})

        self.X_test['dow'] = self.X_test['inspection_date'].dt.day_name().str.lower()
        self.X_test['month'] = self.X_test['inspection_date'].dt.month
        self.X_test = self.X_test.astype({"month": 'str'})
        
        
    def _step_other(self, df_train, df_test, cols, thresh = 0.002):
    
        others_dict = {}
        df_train_copy = df_train.copy()
    
        df_test_copy = df_test.copy()
    
        for col in cols:
            # Obtener los niveles que pasan el thresh con train
            lvls = df_train[col].value_counts(normalize=True)[df_train[col].value_counts(normalize=True) > thresh].index.array
            # Almacenar los niveles en un dict
            others_dict[col] = lvls
            # Hacer el cambio a 'other' en el df_train
            df_train_copy[col] = df_train[col].mask(~df_train[col].isin(lvls), 'other')
            # Hacer el  cambio a 'other'en el df_test
            df_test_copy[col] = df_test[col].mask(~df_test[col].isin(lvls), 'other')
        
        return df_train_copy, df_test_copy, others_dict    
    
        
    def _change_vars_other(self):
        self.X_train, self.X_test, self.other_dict = self._step_other(self.X_train, self.X_test, ['inspection_type', 'facility_type','zip'])

    def _scale_vars(self):
        scaler = MinMaxScaler()
        self.X_train[['longitude','latitude']] = scaler.fit_transform(self.X_train[['longitude','latitude']])
        self.X_test[['longitude','latitude']] = scaler.transform(self.X_test[['longitude','latitude']])
        self.trained_scaler = scaler
        
    def _encode_data_onehot(self):
        encoder = OneHotEncoder(sparse = False)

        onehot_train = pd.DataFrame(encoder.fit_transform(self.X_train[['inspection_type', 'facility_type', 'zip', 'risk','dow','month']]))
        onehot_train.columns = encoder.get_feature_names(['inspection_type', 'facility_type', 'zip', 'risk','dow','month'])
        onehot_train

        self.X_train.drop(columns = ['inspection_type', 'facility_type', 'zip', 'risk','dow','month'], inplace= True)
        self.X_train = pd.concat([self.X_train, onehot_train], axis = 1)

        onehot_test = pd.DataFrame(encoder.transform(self.X_test[['inspection_type', 'facility_type', 'zip', 'risk','dow','month']]))
        onehot_test.columns = encoder.get_feature_names(['inspection_type', 'facility_type', 'zip', 'risk','dow','month'])
        onehot_test

        self.X_test.drop(columns = ['inspection_type', 'facility_type', 'zip', 'risk','dow','month'], inplace= True)

        self.X_test = pd.concat([self.X_test.reset_index(drop = True), onehot_test], axis = 1)
        
        self.trained_encoder = encoder


    def _drop_useless_cols(self):
        self.X_train.drop(columns = ['inspection_id', 'inspection_date', 'violations'], inplace = True)
        self.X_test.drop(columns = ['inspection_id', 'inspection_date', 'violations'], inplace = True)

    def _change_labels_y(self):
        self.y_train = self.y_train.apply(lambda x: 1 if x in ['pass'] else 0 ) 
        self.y_test = self.y_test.reset_index(drop = True).apply(lambda x: 1 if x in ['pass'] else 0 )
        
    def _save_df(self):
        local_path = get_file_path(self.historic, self.query_date, self.prefix)
        pickle.dump(self.df, open(local_path, 'wb'))
        print(f"Succesfully saved temp file as pickle in: {local_path}")

        
    # Luigi's interface methods
    def generate_features(self, save=False):
        """
        Genera el self.df a partir de los datos post-limpieza (los toma de S3)
        Este self.df ya debe contener todos los features que se quieren agregar
        para entrenar y predecir
        :param save: booleano que determina si se guarda localmente o no el df
        """
        
        # Agrega código MH
        self._split_data()
        self._get_date_features()
        self._change_vars_other()
        self._scale_vars()
        self._encode_data_onehot()
        self._drop_useless_cols()
        self._change_labels_y()
        #
        self.final_rows, self.final_cols = self.X_train.shape #Ojo con esto
        if save:
            self._save_df()

    def get_featured_df(self):
        """
        Regresa el atributo df de la clase. Esta función debe llamarse después de
        generate_features
        :return: data frame con los features añadidos
        """
        self.generate_features()
        #return self.df
        return self.X_train, self.X_test, self.y_train, self.y_test
    
    def get_trained_preprocess(self):
        """
        Obtener los preprocessors entrenados con X_train
        Para poder aplicarse a las ingestas consecutivas
        Obtiene:
            el diccionario de niveles para step_other
            el scaler entrenado
            el enconder entrenado
        """
        self.generate_features()
    
        return self.other_dict, self.trained_scaler, self.trained_encoder

    def get_feature_engineering_metadata(self):
        """
        Genera los metadatos del proceso de generación de features y los regresa como una
        lista de tuplas para ser escritos en una base de datos
        :return: lista de tuplas con metadatos
        """
        if self.query_date is None:
            self.query_date = datetime.datetime.now()

        return [(self.original_rows, self.original_cols, self.final_rows,
                 self.final_cols, self.historic, self.query_date)]
