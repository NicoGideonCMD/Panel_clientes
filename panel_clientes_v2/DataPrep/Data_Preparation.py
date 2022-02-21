import sys
sys.path.append('panel_clientes_v2')
sys.path.append('panel_clientes_v2')

from Help_Dict import dict_marcas_sitios, dict_data_prep, dict_marcas_sitios, dict_canal
from Help_Function import function as ft

from datetime import date
import pandas as pd

class Data_Prep:

    def __init__(self, df, canal):
        self.data = df
        self.canal = canal

    def df_general_agg(self):

        if self.canal == 'BM':
            df_clean = ft.data_prep_bm(self.data, 'prm_email', 'prm_rut','boleta')

            df_email_clean = ft.emaiL_classification(df_clean, 'prm_email', dict_marcas_sitios.dict_correos['list_email'],
                                                     dict_marcas_sitios.dict_correos['forus_email'], dict_marcas_sitios.dict_correos['trash_email'],
                                                     dict_marcas_sitios.dict_correos['trash_mailpag'])

            df_bm = ft.rename_columns(df_email_clean,dict_data_prep.dict_rename_bm)

            agg_columns = ['cadena', 'tienda', 'fecha', 'email', 'rut', 'tipo_correo']

            df_day = df_bm.groupby(agg_columns).agg({'venta_neta':'sum', 'boleta':'nunique', 'cod_prod':'count'}).reset_index().rename(columns={'venta_neta':'monto_total_dia', 'boleta':'cantidad_boleta', 'cod_prod':'unidades_dia'})

        elif self.canal == 'ECOM':
            df_clean = ft.data_prep_ecom(self.data, 'email', 'monto_total', 'descuento_total')
            df_email_clean = ft.emaiL_classification(df_clean, 'email', dict_marcas_sitios.dict_correos['list_email'], dict_marcas_sitios.dict_correos['forus_email'],
                                                     dict_marcas_sitios.dict_correos['trash_email'],
                                                     dict_marcas_sitios.dict_correos['trash_mailpag'])

            df_ecom = ft.rename_columns(df_email_clean, dict_data_prep.dict_rename_ecom)
            df_ecom['fecha'] = pd.to_datetime(df_ecom['fecha_hora']).dt.date
            agg_columns = ['sitio', 'fecha', 'email', 'tipo_correo']

            df_day = df_ecom.groupby(agg_columns).agg(
                {'monto_total': 'sum', 'orden': 'nunique', 'unidades': 'sum'}).reset_index().rename(
                columns={'monto_total': 'monto_total_dia', 'orden': 'cantidad_boleta', 'unidades': 'unidades_dia'})


        return df_day

    def cliente_activo_perdido(self, df, dias_usar):

        if self.canal == 'BM':
            now = pd.to_datetime(date.today())
            dias = dias_usar
            df_day_filtro_correo = df[df.email != 'sin_informacion']
            df_agg_fecha_ult = df_day_filtro_correo.groupby(['cadena', 'tienda', 'email']).agg(
                {'monto_total_dia': 'sum', 'cantidad_boleta': 'sum', 'unidades_dia': 'sum',
                 'fecha': 'max'}).reset_index()

            df_agg_fecha_ult['diff_now'] = (now - pd.to_datetime(df_agg_fecha_ult['fecha'])).dt.days
            df_agg_fecha_ult['activo_perdido'] = df_agg_fecha_ult.apply(lambda row: 'perdido' if row['diff_now'] > dias
                                                                                else 'activo', axis=1)

        elif self.canal == 'ECOM':
            now = pd.to_datetime(date.today())
            dias = dias_usar
            df_day_filtro_correo = df[df.email != 'sin_informacion']

            df_agg_fecha_ult = df_day_filtro_correo.groupby(['sitio', 'email']).agg(
                {'monto_total_dia': 'sum', 'cantidad_boleta': 'sum', 'unidades_dia': 'sum',
                 'fecha': 'max'}).reset_index()

            df_agg_fecha_ult['diff_now'] = (now - pd.to_datetime(df_agg_fecha_ult['fecha'])).dt.days
            df_agg_fecha_ult['activo_perdido'] = df_agg_fecha_ult.apply(lambda row: 'perdido' if row['diff_now'] > dias
                                                                                else 'activo', axis=1)


        return df_agg_fecha_ult

    def tipo_cliente(self, df, meses):

        if self.canal == 'BM':

            df_purchase = ft.previous_purchase(df, 'cadena', 'email', 'fecha','monto_total_dia', 'unidades_dia', 'cantidad_boleta', 'tipo_correo')

            df_purchase['tipo_cliente'] = df_purchase.apply(lambda row: ft.tipo_cliente(row['fecha'], row['fecha_compra_anterior'],
                                                                    row['mes_actual'], row['mes_compra_anterior'], row['anio_actual'], row['anio_compra_anterior'], meses), axis=1)


            var_output_bm = ['cadena', 'email', 'tipo_cliente', 'tipo_correo', 'fecha', 'monto_total_dia','unidades_dia','cantidad_boleta']

            df_output = df_purchase[var_output_bm]
            df_output['year'] = df_output['fecha'].dt.year
            df_output['month'] = df_output['fecha'].dt.month
            df_output['meses_tipo_cliente'] = meses

        elif self.canal == 'ECOM':

            df_purchase = ft.previous_purchase(df, 'sitio', 'email', 'fecha','monto_total_dia', 'unidades_dia', 'cantidad_boleta', 'tipo_correo')

            df_purchase['tipo_cliente'] = df_purchase.apply(lambda row: ft.tipo_cliente(row['fecha'], row['fecha_compra_anterior'],
                                                                    row['mes_actual'], row['mes_compra_anterior'], row['anio_actual'], row['anio_compra_anterior'], meses), axis=1)


            var_output_bm = ['sitio', 'email', 'tipo_cliente', 'tipo_correo', 'fecha', 'monto_total_dia','unidades_dia','cantidad_boleta']

            df_output = df_purchase[var_output_bm]
            df_output['year'] = df_output['fecha'].dt.year
            df_output['month'] = df_output['fecha'].dt.month
            df_output['meses_tipo_cliente'] = meses

        return df_output