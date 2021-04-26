#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 24 11:54:10 2021

@author: mario
"""

#Deprecated functions

# class DataEngineer:
    
    def _extract_violation_num(self, s) -> int:
        """
        Extrae el número de violación en un string.
        :param s: string
                      texto del registro
        :return: int
        """
        pattern = '\d+\.'  # numeros + punto # cambio para coincidir con formato de violations
        result = re.findall(pattern, s)
        if not result:  # no hay violaciones
            return 0
        else:  # regresa número de violación
            return int(result[0].replace('.', '')) # cambio

    def _get_violations_incurred(self, s) -> list:
        """
        Extra el total de violaciones en un registro.
        :param s: string
                      Una celda de la columna 'violations'
        :return violation_nums: list
                 lista con todas las infracciones, por ejemplo, [13,22,55]
        """
        all_violations = s.split(' | ')
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