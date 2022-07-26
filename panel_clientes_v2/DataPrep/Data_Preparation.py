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

            df_sales_agg = df_bm.groupby(agg_columns).agg({'venta_neta':'sum', 'boleta':'nunique', 'cod_prod':'count'}).reset_index().rename(columns={'venta_neta':'monto_total_dia'
                                                                                                                                              , 'boleta':'cantidad_boleta', 'cod_prod':'unidades_dia'})
            agg_columns_min_date = ['cadena', 'tienda', 'email', 'rut', 'tipo_correo']

            df_min_date = df_bm.groupby(agg_columns_min_date).agg({'fecha':'min'}).reset_index().rename(columns={'fecha':'min_fecha'})

            df_day = pd.merge(df_sales_agg, df_min_date, how='left', on=['cadena', 'tienda', 'email', 'rut', 'tipo_correo'])

            return df_day

        elif self.canal == 'ECOM':
            df_clean = ft.data_prep_ecom(self.data, 'email', 'monto_total', 'descuento_total')
            df_email_clean = ft.emaiL_classification(df_clean, 'email', dict_marcas_sitios.dict_correos['list_email'], dict_marcas_sitios.dict_correos['forus_email'],
                                                     dict_marcas_sitios.dict_correos['trash_email'],
                                                     dict_marcas_sitios.dict_correos['trash_mailpag'])

            df_ecom = ft.rename_columns(df_email_clean, dict_data_prep.dict_rename_ecom)
            df_ecom['fecha'] = pd.to_datetime(df_ecom['fecha_hora']).dt.date
            agg_columns = ['sitio', 'fecha', 'email', 'rut','tipo_correo']

            df_sales_agg = df_ecom.groupby(agg_columns).agg(
                {'monto_total': 'sum', 'orden': 'nunique', 'unidades': 'sum'}).reset_index().rename(
                columns={'monto_total': 'monto_total_dia', 'orden': 'cantidad_boleta', 'unidades': 'unidades_dia'})

            agg_columns_min_date = ['sitio', 'email', 'rut','tipo_correo']

            df_min_date = df_ecom.groupby(agg_columns_min_date).agg({'fecha': 'min'}).reset_index().rename(columns={'fecha': 'min_fecha'})

            df_day = pd.merge(df_sales_agg, df_min_date, how='left', on=['sitio', 'email', 'rut','tipo_correo'])

            return df_day

        elif self.canal == 'ALL':
            df_clean = ft.data_prep_all(self.data, 'email', 'venta_neta', 'rut', 'boleta', 'tienda')
            df_clean['tienda'] = df_clean.apply(lambda row: dict_marcas_sitios.dict_marcas_sitios[row['tienda']], axis=1)
            df_email_clean = ft.emaiL_classification(df_clean, 'email', dict_marcas_sitios.dict_correos['list_email'],
                                                     dict_marcas_sitios.dict_correos['forus_email'],
                                                     dict_marcas_sitios.dict_correos['trash_email'],
                                                     dict_marcas_sitios.dict_correos['trash_mailpag'])

            agg_columns = ['tienda', 'email', 'tipo_correo','rut', 'fecha']

            df_sales_agg = df_email_clean.groupby(agg_columns).agg({'boleta':'nunique', 'cod_prod':'count', 'venta_neta':'sum'}).reset_index().rename(
                columns={'boleta':'cantidad_boleta', 'cod_prod':'unidades_dia', 'venta_neta':'monto_total_dia'})

            agg_columns_min_date = ['tienda', 'email', 'tipo_correo','rut']

            df_min_date = df_email_clean.groupby(agg_columns_min_date).agg({'fecha': 'min'}).reset_index().rename(columns={'fecha': 'min_fecha'})

            df_day = pd.merge(df_sales_agg, df_min_date, how='left', on=['tienda', 'email', 'tipo_correo','rut'])

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

        elif self.canal == 'ALL':
            now = pd.to_datetime(date.today())
            dias = dias_usar
            df_day_filtro_correo = df[df.email != 'sin_informacion']

            df_agg_fecha_ult = df_day_filtro_correo.groupby(['tienda', 'email', 'rut']).agg(
                {'monto_total_dia': 'sum', 'cantidad_boleta': 'sum', 'unidades_dia': 'sum',
                 'fecha': 'max'}).reset_index()

            df_agg_fecha_ult['diff_now'] = (now - pd.to_datetime(df_agg_fecha_ult['fecha'])).dt.days
            df_agg_fecha_ult['activo_perdido'] = df_agg_fecha_ult.apply(lambda row: 'perdido' if row['diff_now'] > dias
                                                                                else 'activo', axis=1)


        return df_agg_fecha_ult

    def tipo_cliente(self, df, meses):

        if self.canal == 'BM':

            df_purchase = ft.previous_purchase(df, 'cadena', 'email', 'fecha','monto_total_dia', 'unidades_dia', 'cantidad_boleta', 'tipo_correo', 'min_fecha')

            df_purchase['tipo_cliente'] = df_purchase.apply(lambda row: ft.tipo_cliente(row['fecha'], row['fecha_compra_anterior'],
                                                                   row['mes_actual'], row['mes_compra_anterior'] ,row['anio_actual'],
                                                                   row['anio_compra_anterior'], meses, row['fecha_compra_pre_anterior'],
                                                                   row['min_fecha']), axis=1)


            var_output_bm = ['cadena', 'email', 'rut','tipo_cliente', 'tipo_correo', 'fecha', 'monto_total_dia','unidades_dia','cantidad_boleta']

            df_output = df_purchase[var_output_bm]
            df_output['year'] = df_output['fecha'].dt.year
            df_output['month'] = df_output['fecha'].dt.month
            df_output['meses_tipo_cliente'] = meses

        elif self.canal == 'ECOM':

            df_purchase = ft.previous_purchase(df, 'sitio', 'email', 'fecha','monto_total_dia', 'unidades_dia', 'cantidad_boleta', 'tipo_correo', 'min_fecha')

            df_purchase['tipo_cliente'] = df_purchase.apply(lambda row: ft.tipo_cliente(row['fecha'], row['fecha_compra_anterior'],
                                                                   row['mes_actual'], row['mes_compra_anterior'],
                                                                   row['anio_actual'], row['anio_compra_anterior'], meses, row['fecha_compra_pre_anterior'],
                                                                   row['min_fecha']), axis=1)


            var_output_bm = ['sitio', 'email', 'rut','tipo_cliente', 'tipo_correo', 'fecha', 'monto_total_dia','unidades_dia','cantidad_boleta']

            df_output = df_purchase[var_output_bm]
            df_output['year'] = df_output['fecha'].dt.year
            df_output['month'] = df_output['fecha'].dt.month
            df_output['meses_tipo_cliente'] = meses

        elif self.canal == 'ALL':

            df_purchase = ft.previous_purchase(df, 'tienda', 'email', 'fecha', 'monto_total_dia', 'unidades_dia', 'cantidad_boleta', 'tipo_correo', 'min_fecha')

            df_purchase['tipo_cliente'] = df_purchase.apply(lambda row: ft.tipo_cliente(row['fecha'], row['fecha_compra_anterior'],
                                                                   row['mes_actual'], row['mes_compra_anterior'], row['anio_actual'],
                                                                   row['anio_compra_anterior'], meses, row['fecha_compra_pre_anterior'],
                                                                   row['min_fecha']), axis=1)


            var_output_all = ['tienda', 'email', 'rut','tipo_cliente', 'tipo_correo', 'fecha', 'monto_total_dia','unidades_dia','cantidad_boleta']

            df_output = df_purchase[var_output_all]
            df_output['year'] = df_output['fecha'].dt.year
            df_output['month'] = df_output['fecha'].dt.month
            df_output['meses_tipo_cliente'] = meses

        return df_output

    def expand_date(self):
        df = self.data
        df['fecha'] = df.apply(lambda row: pd.date_range(row['fecha_primera_compra'], row['fecha_ultima_compra']), axis=1)
        df_explode = df.explode('fecha')
        df_explode = df_explode.drop(columns=['fecha_primera_compra', 'fecha_ultima_compra'])

        return df_explode

    def df_general_clv(self):

        if self.canal == 'BM':
            df_clean = ft.data_prep_bm(self.data, 'prm_email', 'prm_rut','boleta')
            # renombrar columnas
            df_clean_renamed = df_clean.rename(columns = {'prm_email':'email', 'prm_rut':'rut','cadena_lv':'tienda','boleta':'orden','venta_neta':'monto_total'})

            #filtrar tipo_correo = correo_cliente
            df_email_clean = ft.emaiL_classification(df_clean_renamed, 'email',
                                                     dict_marcas_sitios.dict_correos['list_email'],
                                                     dict_marcas_sitios.dict_correos['forus_email'],
                                                     dict_marcas_sitios.dict_correos['trash_email'],
                                                     dict_marcas_sitios.dict_correos['trash_mailpag'])
            # homologar cadena tienda
            df_email_clean['cadena'] = df_email_clean['tienda'].apply(lambda row: ft.homo_tienda(row, dict_marcas_sitios.dict_tienda))

            return df_email_clean


        elif self.canal == 'ECOM':
            df_clean = ft.data_prep_ecom(self.data, 'email', 'monto_total', 'descuento_total')
            df_email = ft.emaiL_classification(df_clean, 'email', dict_marcas_sitios.dict_correos['list_email'],
                                                     dict_marcas_sitios.dict_correos['forus_email'],
                                                     dict_marcas_sitios.dict_correos['trash_email'],
                                                     dict_marcas_sitios.dict_correos['trash_mailpag'])
            df_email_clean = df_email[df_email.tipo_correo=='correo_cliente']
            df_email_clean['cadena'] = df_email_clean['tienda'].apply(lambda row: ft.homo_tienda(row, dict_marcas_sitios.dict_tienda))

            return df_email_clean

        elif self.canal == 'ALL':
            df_clean = ft.data_prep_all(self.data, 'email', 'venta_neta', 'rut', 'boleta', 'tienda')
            df_clean_renamed = df_clean.rename(columns={'boleta': 'orden','venta_neta': 'monto_total'})
            df_email = ft.emaiL_classification(df_clean_renamed, 'email', dict_marcas_sitios.dict_correos['list_email'],
                                                     dict_marcas_sitios.dict_correos['forus_email'],
                                                     dict_marcas_sitios.dict_correos['trash_email'],
                                                     dict_marcas_sitios.dict_correos['trash_mailpag'])
            df_email_clean = df_email[df_email.tipo_correo=='correo_cliente']
            df_email_clean['cadena'] = df_email_clean['tienda'].apply(lambda row: ft.homo_tienda(row, dict_marcas_sitios.dict_tienda))

            return df_email_clean

