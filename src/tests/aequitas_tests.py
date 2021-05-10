import unittest
from src.pipeline.bias_fairness import MrFairness
from src.utils.general import get_file_path, load_from_pickle


 aeq_df_test = pd.DataFrame({'score': [0,1,1,0], 'label_value': [0,0,1,0],
                                         'facility_type': ['restaurant', 'canvas','bar']})


class AequitasTester(unittest.TestCase):

    def __init__(self, historic, query_date, training, *args, **kwargs):
        super(AequitasTester, self).__init__(*args, **kwargs)
        self.historic = historic
        self.query_date = query_date
        self.training = training
        # CBP
        self.num_attributes
        self.test_metrics_value

    def setUp(self):
        # getting local pickle
        file_path = get_file_path(historic=self.historic, query_date=self.query_date,
                                  prefix=DataEngineer.prefix, training=self.training)
        data = load_from_pickle(file_path)

        if self.training:
            self.group = data['group']
            self.bias = data['bias']
            self.fairness = data['fairness']

    def test_df_not_empty(self):
        # fue el único test  que agregué porque en teoría feature engineering debería permitir tanto quitar como
        # poner columnas; o incluso renglones
        if self.training:
            assert self.group['counts'].shape[0] != 0, "Group counts is empty"
            assert self.group['percentage'].shape[0] != 0, "Group percentage is empty"
            assert self.bias['dataframe'].shape[0] != 0, "Bias df is empty"
            assert self.bias['summary'].shape[0] != 0, "Bias small df is empty"
            assert self.fairness['group'].shape[0] != 0, "Fairness by group df is empty"
            assert self.fairness['attributes'].shape[0] != 0, "Fairness by attributes df is empty"
            assert self.fairness['overall'].shape[0] != 0, "Overall fairness is empty"
            
        else:
            assert 100 == 100, "We are not in training so it doesn't make sense to test this."

    def runTest(self):
        print("Corriendo tests de limpieza de datos...")
        self.test_df_not_empty()
        print(">>>> Tests de limpieza terminados <<<<")


## Prueba donde falla aequitas: 
# Es necesario definir un grupo (en este caso es facility_type), podemos probar que el número de categorías de facility type sea mayor
# Para ello, necesitamos cargar Group(), utilizar método get_crosstabss() y nos genera xtab y attrbs, por lo que probamos que len de attrbs sea mayor y truene
# Como se construyó aequitas df (según el script con la clase MrFairness), solo se alimenta con la feature de facility_type, por lo tanto, sabemos que attrbs 
# a fuerzas será de tamaño 1 o solo tendrá el elemento facility_type, por lo tanto si probamos que len(attrbs) != 1 entonces debería fallar la prueba
# y arrojar mensaje: "There are more features than used in aequitas framework"

# Deberíamos cargar el data frame de aequitas o generar uno de prueba


# CBP
    def test_num_attributes(self):
        g=Group()
        self.all_metrics_df, self.attributes = group.get_crosstabs(aeq_df_test)
        assert self.num_attributes == len(self.attributes), 'Error: There are more than 1 attributes to evaluate'
    

## Otra prueba puede ser que alguna metrica sea mayor a 1 dado que son porcentajes
# CBP

     def test_metrics_value(self):
        g=Group()
        self.all_metrics_df, self.attributes = group.get_crosstabs(aeq_df_test)
        assert self.all_metrics_df['fpr'] <= 1, 'Error: metric values must be a number between 0 and 1'