import pickle
import re
import pandas as pd
import datetime
import os

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler 
from sklearn.preprocessing import OneHotEncoder

from src.utils.general import get_pickle_from_s3_to_pandas, get_file_path
from src.utils.general import load_from_pickle, save_to_pickle


class DataCleaner:

    # static variables
    prefix = 'clean'

    def __init__(self, historic=False, query_date=None):
        self.df = get_pickle_from_s3_to_pandas(historic, query_date)
        self.historic = historic
        self.query_date = query_date
        self.prefix = DataCleaner.prefix

    def _subset_cols(self):

        if self.historic:
            self.df = self.df[['inspection_id', 'facility_type', 'risk', 'zip', 'inspection_date',
                               'inspection_type', 'results', 'violations', 'latitude', 'longitude']]
        else:
            self.df = self.df[['inspection_id', 'license_', 'facility_type', 'risk', 'zip', 'inspection_date',
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
        self.df['Pass'] = 'pass'
        self.df['results'] = self.df['Pass'].where(self.df['results'].isin(['Pass', 'Pass w/ Conditions']), 'not_pass')
        self.df.drop(columns = ['Pass'], inplace = True)

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
        self.df.dropna(axis=0, inplace=True)
        self._change_data_types()
        self._clean_results()
        self._standardize_column_strings(['facility_type', 'risk', 'inspection_type'])
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
        #self.clean_data()
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

    def __init__(self, historic=False, query_date=None, training = True):
        self.historic = historic
        self.query_date = query_date
        self.training = training
        self.df = self._get_df()
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

        if self.training:
            self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(X, y, shuffle = False)
            
        else:
            self.X_consec = X
            self.y_consec = y
         
    def _get_date_features(self):
        
        if self.training:
        
            self.X_train['dow'] = self.X_train['inspection_date'].dt.day_name().str.lower()
            self.X_train['month'] = self.X_train['inspection_date'].dt.month
            self.X_train = self.X_train.astype({"month": 'str'})
            
            self.X_test['dow'] = self.X_test['inspection_date'].dt.day_name().str.lower()
            self.X_test['month'] = self.X_test['inspection_date'].dt.month
            self.X_test = self.X_test.astype({"month": 'str'})
        
        else:
            
            self.X_consec['dow'] = self.X_consec['inspection_date'].dt.day_name().str.lower()
            self.X_consec['month'] = self.X_consec['inspection_date'].dt.month
            self.X_consec = self.X_consec.astype({"month": 'str'})
            
        
    def _step_other(self, df_train, df_test, cols, thresh = 0.002):
    
        if self.training:
    
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

        else:
            
            others_dict = self.other_dict
            df_test_copy = df_test.copy()
        
            for col in cols:
                lvls = others_dict[col]
                df_test_copy[col] = df_test[col].mask(~df_test[col].isin(lvls), 'other')
        
            return df_test_copy    
    
        
    def _change_vars_other(self):
        
        if self.training:
            self.X_train, self.X_test, self.other_dict = self._step_other(self.X_train, self.X_test, ['inspection_type', 'facility_type','zip'])
            
        else:
            self.X_consec = self._step_other(None, self.X_consec, ['inspection_type', 'facility_type','zip'], self.other_dict)

    def _scale_vars(self):
        
        if self.training:
            scaler = MinMaxScaler()
            self.X_train[['longitude','latitude']] = scaler.fit_transform(self.X_train[['longitude','latitude']])
            self.X_test[['longitude','latitude']] = scaler.transform(self.X_test[['longitude','latitude']])
            self.trained_scaler = scaler
            
        else:
            scaler = self.trained_scaler
            self.X_consec[['longitude','latitude']] = scaler.transform(self.X_consec[['longitude','latitude']])
        
    def _encode_data_onehot(self):
        
        if self.training:
            
            inspection_types = ['canvas', 'complaint', 'consultation',
                    'license','suspected_food_poisoning', 'tag_removal', 'other']

            facility_types =['assisted_living', 'bakery', 'bar', 'catering',
                             'childrens_service_facility', 'daycare', 'grocery_store',
                             'hospital', 'liquor', 'na', 'other_food', 'restaurant',
                             'school', 'wholesale', 'other']

            zips = ['60601', '60602', '60603', '60604', '60605', '60606', '60607',
                    '60608', '60609', '60610', '60611', '60612', '60613', '60614',
                    '60615', '60616', '60617', '60618', '60619', '60620', '60621',
                    '60622', '60623', '60624', '60625', '60626', '60628', '60629',
                    '60630', '60631', '60632', '60634', '60636', '60637', '60638',
                    '60639', '60640', '60641', '60642', '60643', '60644', '60645',
                    '60646', '60647', '60649', '60651', '60652', '60653', '60654',
                    '60655', '60656', '60657', '60659', '60660', '60661', '60666',
                    '60707', 'other']

            risks = ['risk_1_high', 'risk_2_medium', 'risk_3_low']

            dows = ['monday', 'tuesday','wednesday', 'thursday', 'friday', 'saturday', 'sunday']

            months =['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
            
            encoder = OneHotEncoder(sparse = False,
                        categories= [inspection_types, facility_types,
                                     zips, risks, dows, months],
                        handle_unknown ='ignore')

            onehot_train = pd.DataFrame(encoder.fit_transform(self.X_train[['inspection_type', 'facility_type', 'zip', 'risk','dow','month']]))
            onehot_train.columns = encoder.get_feature_names(['inspection_type', 'facility_type', 'zip', 'risk','dow','month'])

            self.X_train.drop(columns = ['inspection_type', 'facility_type', 'zip', 'risk','dow','month'], inplace= True)
            self.X_train = pd.concat([self.X_train, onehot_train], axis = 1)

            onehot_test = pd.DataFrame(encoder.transform(self.X_test[['inspection_type', 'facility_type', 'zip', 'risk','dow','month']]))
            onehot_test.columns = encoder.get_feature_names(['inspection_type', 'facility_type', 'zip', 'risk','dow','month'])

            self.X_test.drop(columns = ['inspection_type', 'facility_type', 'zip', 'risk','dow','month'], inplace= True)
            
            self.X_test = pd.concat([self.X_test.reset_index(drop = True), onehot_test], axis = 1)
        
            self.trained_encoder = encoder
            
        else:
            
            encoder = self.trained_encoder
            
            onehot_test = pd.DataFrame(encoder.transform(self.X_consec[['inspection_type', 'facility_type', 'zip', 'risk','dow','month']]))
            onehot_test.columns = encoder.get_feature_names(['inspection_type', 'facility_type', 'zip', 'risk','dow','month'])
            
            self.X_consec = pd.concat([self.X_consec, onehot_test], axis = 1)

            self.X_consec.drop(columns = ['inspection_type', 'facility_type', 'zip', 'risk','dow','month'], inplace= True)


    def _drop_useless_cols(self):
        
        if self.training:
            self.X_train.drop(columns = ['inspection_id', 'inspection_date', 'violations'], inplace = True)
            self.X_test.drop(columns = ['inspection_id', 'inspection_date', 'violations'], inplace = True)
        else:
            self.X_consec.drop(columns = ['inspection_date', 'violations'], inplace=True)

    def _change_labels_y(self):
        
        if self.training:
            self.y_train = self.y_train.apply(lambda x: 1 if x in ['pass'] else 0 ) 
            self.y_test = self.y_test.reset_index(drop = True).apply(lambda x: 1 if x in ['pass'] else 0 )
            
        else:
            self.y_consec = self.y_consec.apply(lambda x: 1 if x in ['pass'] else 0 ) 
        
    def _save_df(self):

        if self.training:
        
            compact_data = {'X_train': self.X_train, 'X_test': self.X_test, 'y_train': self.y_train, 'y_test': self.y_test}
            
            local_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=self.prefix, training=self.training)
            pickle.dump(compact_data, open(local_path, 'wb'))
            print(f"Succesfully saved temp file as pickle in: {local_path}")
            
        else:
            
            compact_data = {'X_consec': self.X_consec, 'y_consec': self.y_consec}
            local_path = get_file_path(historic=self.historic, query_date=self.query_date, prefix=self.prefix, training=self.training)
            pickle.dump(compact_data, open(local_path, 'wb'))
            print(f"Succesfully saved temp file as pickle in: {local_path}")
            
            
    def _save_transformers(self):
        
        if self.training:
            
            root_path = os.getcwd()
        
            path = f"{root_path}/temp/other_dict.pkl"
            save_to_pickle(self.other_dict, path)
        
            path = f"{root_path}/temp/trained_scaler.pkl"
            save_to_pickle(self.trained_scaler, path)
        
            path = f"{root_path}/temp/trained_encoder.pkl"
            save_to_pickle(self.trained_encoder, path)
                
            print(f"Succesfully saved transformers as pickles in: {root_path}/temp/") 
            
        else:
            
            print("Transformers are not saved when Training = False")
            
        
    # Luigi's interface methods
    def generate_features(self, save_df=False, save_transformers = True):
        """
        Genera el self.df a partir de los datos post-limpieza (los toma de S3)
        Este self.df ya debe contener todos los features que se quieren agregar
        para entrenar y predecir
        :param save: booleano que determina si se guarda localmente o no el df
        """
        
        if self.training is False:
            root_path = os.getcwd()
            path = f"{root_path}/temp/other_dict.pkl"
            self.other_dict = load_from_pickle(path)
        
            path = f"{root_path}/temp/trained_scaler.pkl"
            self.trained_scaler = load_from_pickle(path)
        
            path = f"{root_path}/temp/trained_encoder.pkl"
            self.trained_encoder = load_from_pickle(path)
            
        self._split_data()
        self._get_date_features()
        self._change_vars_other()
        self._scale_vars()
        self._encode_data_onehot()
        self._drop_useless_cols()
        self._change_labels_y()

        if self.training:
            self.final_rows = self.X_train.shape[0] + self.X_test.shape[0]
            
            self.final_cols = self.X_train.shape[1] + 1
                
        else:
            self.final_rows, self.final_cols = self.X_consec.shape #Ojo con esto
            self.final_cols += 1
            
        if save_df:
            self._save_df()
            
        if save_transformers:
            self._save_transformers()
            

    def get_featured_df(self):
        """
        Regresa el atributo df de la clase. Esta función debe llamarse después de
        generate_features
        :return: data frame con los features añadidos
        """
        #self.generate_features()
        
        if self.training:
            return self.X_train, self.X_test, self.y_train, self.y_test
    
        else:
            return self.X_consec, self.y_consec
            
    
    def get_trained_transformers(self):
        """
        Obtener los transformers entrenados con X_train
        Para poder aplicarse a las ingestas consecutivas
        Obtiene:
            el diccionario de niveles para step_other
            el scaler entrenado
            el enconder entrenado
        """
        if self.training:
            print("Fetching recently trained transformers...")
        
            #self.generate_features()
            
        else:
            print("Loading locally saved transformers...")
                
            root_path = os.getcwd()
        
            path = f"{root_path}/temp/other_dict.pkl"
            self.other_dict = load_from_pickle(path)
        
            path = f"{root_path}/temp/trained_scaler.pkl"
            self.trained_scaler = load_from_pickle(path)
        
            path = f"{root_path}/temp/trained_encoder.pkl"
            self.trained_encoder = load_from_pickle(path)
            
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
                 self.final_cols, self.historic, self.query_date, self.training)] #Toño
