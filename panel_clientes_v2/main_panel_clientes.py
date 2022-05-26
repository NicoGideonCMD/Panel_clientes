### Scripts ###
from DataLoader import loader_data
from Help_Dict import dict_connection_aws, dict_query_ventas, dict_data_prep, dict_marcas_sitios, dict_clv
from Help_Function import function
from DataPrep import Data_Preparation
from Querys import querys

### Libs ###
import pandas as pd
import datetime
import warnings
import argparse
import pyarrow

pd.set_option('display.max_columns',None)
warnings.filterwarnings("ignore")

####### 1.- Extract Data to Redshift ###########
print('Inicio de la extracción de data')
aws_connection = dict_connection_aws.connection_aws
conexion_aws = function.connection_aws(aws_connection['dbname'],
                         aws_connection['host'],
                         aws_connection['port'],
                         aws_connection['user'],
                         aws_connection['passw']
                         )


cursor = conexion_aws.cursor()

data_loader = loader_data.loader(conexion_aws)


tinicio_ecom = datetime.datetime.now()

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--mode',
                    required=True,
                    choices=['BM', 'ECOM','ALL'],
                    help='Selecciona el canal en que se ejecuatara el panel BM o ECOM')

args = parser.parse_args()
execution_mode = args.mode
print(execution_mode)


if execution_mode == 'ECOM':
    df_data = data_loader.read_query(querys.query_ecommerce.format(dict_query_ventas.dict_ecommerce['fecha_inicio_ecom'],
                                                                   dict_query_ventas.dict_ecommerce['fecha_fin_ecom']))

    df_data['tienda'] = df_data.apply(lambda row: dict_marcas_sitios.dict_sitios[row['tienda']], axis=1)
    tfin_ecom = datetime.datetime.now() - tinicio_ecom
    print('query ecommerce se demora: {}'.format(tfin_ecom))

elif execution_mode == 'BM':
    tinicio_bm = datetime.datetime.now()
    df_data = data_loader.read_query(querys.query_brick_mortar.format(dict_query_ventas.dict_bm['fecha_inicio_bm'],
                                                                      dict_query_ventas.dict_bm['fecha_fin_bm']))
    tfin_bm = datetime.datetime.now() - tinicio_bm
    print('query B&M se demora: {}'.format(tfin_bm))

elif execution_mode == 'ALL':
    tinicio_all = datetime.datetime.now()

    df_data_bm = data_loader.read_query(querys.query_bm_all.format(dict_query_ventas.dict_bm['fecha_inicio_bm'],
                                                                   dict_query_ventas.dict_bm['fecha_fin_bm']))

    df_data_ecom = data_loader.read_query(querys.query_ecom_all.format(dict_query_ventas.dict_ecommerce['fecha_inicio_ecom'],
                                                                      dict_query_ventas.dict_ecommerce['fecha_fin_ecom']))


    df_data_bm['rut'] = df_data_bm.apply(lambda row: function.dv(row['prm_rut']), axis=1)
    df_data_bm['boleta'] = df_data_bm['boleta'].astype('str')
    df_data_bm = df_data_bm.dropna()

    columns = ['cadena_lv', 'prm_email', 'rut', 'boleta', 'fecha', 'cod_prod', 'venta_neta', 'canal']

    df_data_bm_sort = df_data_bm[columns]
    df_data_bm_sort = df_data_bm_sort.rename(columns={'cadena_lv': 'tienda', 'prm_email': 'email'})

    df_data_ecom = df_data_ecom.rename(columns={'orden': 'boleta'})
    df_data_ecom['tienda'] = df_data_ecom.apply(lambda row: dict_marcas_sitios.dict_sitios[row['tienda']], axis=1)
    df_data = pd.concat([df_data_ecom, df_data_bm_sort])
    print(df_data.tienda.unique())
    tfin_all = datetime.datetime.now() - tinicio_all
    print('query ALL se demora: {}'.format(tfin_all))

data_loader.close_connection()

####### 2.- Data Preparation ###########
print('Inicio de preparación de la data')

tinicio_prepdatos = datetime.datetime.now()

data_prep = Data_Preparation.Data_Prep(df_data, execution_mode)

df_general = data_prep.df_general_agg()
df_cliente_activo = data_prep.cliente_activo_perdido(df_general, 365)

df_tipo_cliente_12 = data_prep.tipo_cliente(df_general, dict_data_prep.dict_function_tipo_cliente[1])
df_tipo_cliente_18 = data_prep.tipo_cliente(df_general, dict_data_prep.dict_function_tipo_cliente[2])
df_tipo_cliente_24 = data_prep.tipo_cliente(df_general, dict_data_prep.dict_function_tipo_cliente[3])

df_tipo_cliente = pd.concat([df_tipo_cliente_12, df_tipo_cliente_18, df_tipo_cliente_24], ignore_index=True)

tfin_prepdatos = datetime.datetime.now() - tinicio_prepdatos
print('la preparacion de datos se demoro {}'.format(tfin_prepdatos))


####### 3.- Apply RFMT Model ###########
print('Inicio de la agrupación RFMT')

tinicio_rfmt = datetime.datetime.now()

model_rfmt = function.rfmt(df_data, execution_mode)

df_rfmt = model_rfmt


if execution_mode == 'BM':
    df_tipo_cliente_rfmt = pd.merge(df_tipo_cliente, df_rfmt, how='left', on=['cadena', 'email'])

elif execution_mode == 'ECOM':
    df_tipo_cliente_rfmt = pd.merge(df_tipo_cliente, df_rfmt, how='left', on=['sitio', 'email'])

elif execution_mode == 'ALL':
    df_tipo_cliente_rfmt = pd.merge(df_tipo_cliente, df_rfmt, how='left', on=['tienda', 'email'])

tfin_rfmt = datetime.datetime.now() - tinicio_rfmt
print('el proceso del RFMT de datos se demoro {}'.format(tfin_prepdatos))

####### 4.- Apply Classification Model ###########
print('Inicio de la clasificación de clientes')

tinicio_class = datetime.datetime.now()

if execution_mode == 'BM':
    df_percentile = function.percentile25_75(df_tipo_cliente_rfmt, 'cadena', 'year', 'month', 'monetary')
    df_tipo_cliente_percentile = pd.merge(df_tipo_cliente_rfmt, df_percentile, how='left', on=['cadena', 'year', 'month']).fillna(0)
    df_tipo_cliente_percentile['canal'] = 'Brick & Mortar'
    df_tipo_cliente_percentile_vf = df_tipo_cliente_percentile[['canal', 'cadena', 'year', 'month', 'fecha', 'email',
                                                                   'rut','meses_tipo_cliente', 'tipo_correo', 'tipo_cliente',
                                                                   'cantidad_boleta', 'unidades_dia', 'monto_total_dia',
                                                                   'recency', 'frequency', 'monetary', 'tenure', 'p25_monetary', 'p50_monetary',
                                                                   'p75_monetary','upper_whisker', 'lower_whisker', 'iqr']]

elif execution_mode == 'ECOM':
    df_percentile = function.percentile25_75(df_tipo_cliente_rfmt, 'sitio', 'year', 'month', 'monetary')
    df_tipo_cliente_percentile = pd.merge(df_tipo_cliente_rfmt, df_percentile, how='left', on=['sitio', 'year', 'month']).fillna(0)
    df_tipo_cliente_percentile['canal'] = 'Ecommerce'
    df_tipo_cliente_percentile_vf = df_tipo_cliente_percentile[['canal', 'sitio', 'year', 'month', 'fecha', 'email',
                                                                  'rut', 'meses_tipo_cliente', 'tipo_correo',
                                                                   'tipo_cliente', 'cantidad_boleta', 'unidades_dia',
                                                                   'monto_total_dia','recency', 'frequency', 'monetary',
                                                                   'tenure', 'p25_monetary', 'p50_monetary','p75_monetary',
                                                                   'upper_whisker', 'lower_whisker', 'iqr']]


elif execution_mode == 'ALL':
    df_percentile = function.percentile25_75(df_tipo_cliente_rfmt, 'tienda', 'year', 'month', 'monetary')
    df_tipo_cliente_percentile = pd.merge(df_tipo_cliente_rfmt, df_percentile, how='left', on=['tienda', 'year', 'month']).fillna(0)
    df_tipo_cliente_percentile['canal'] = 'All'
    df_tipo_cliente_percentile_vf = df_tipo_cliente_percentile[['canal', 'tienda', 'year', 'month', 'fecha', 'email',
                                                                  'rut', 'meses_tipo_cliente', 'tipo_correo',
                                                                   'tipo_cliente', 'cantidad_boleta', 'unidades_dia',
                                                                   'monto_total_dia','recency', 'frequency', 'monetary',
                                                                   'tenure', 'p25_monetary', 'p50_monetary','p75_monetary',
                                                                   'upper_whisker', 'lower_whisker', 'iqr']]

df_tipo_cliente_percentile_vf['classification'] = df_tipo_cliente_percentile_vf.apply(lambda row: function.class_monetary(int(row['monetary']),
                                                                                                                      int(row['p75_monetary']),
                                                                                                                      int(row['p50_monetary']),
                                                                                                                      int(row['p25_monetary']),
                                                                                                                      int(row['upper_whisker']),
                                                                                                                      int(row['lower_whisker'])), axis=1)

df_tipo_cliente_vf = df_tipo_cliente_percentile_vf.drop(columns={'p75_monetary', 'p50_monetary','p25_monetary', 'upper_whisker', 'lower_whisker', 'iqr'})

tfin_class = datetime.datetime.now() - tinicio_class
print('el proceso del Classification Model finalizo a las {}'.format(tfin_class))


####### 5.- Cálculo CLV ###########
print('Inicio del cálculo de CLV')
tinicio_clv = datetime.datetime.now()

if execution_mode == 'BM':
    df_general_clv = data_prep.df_general_clv()
    clv = function.get_clv(df_general_clv, dict_clv.dict_tasa_clv['tasa'])


elif execution_mode == 'ECOM':
    df_general_clv = data_prep.df_general_clv()
    clv = function.get_clv(df_general_clv, dict_clv.dict_tasa_clv['tasa'])



elif execution_mode == 'ALL':
    df_general_clv = data_prep.df_general_clv()
    clv = function.get_clv(df_general_clv, dict_clv.dict_tasa_clv['tasa'])





tfin_clv = datetime.datetime.now() - tinicio_clv
print('el proceso del cálculo del CLV finalizo a las {}'.format(tfin_clv))

#### Salidas deseadas ####
df_tipo_cliente_vf['rut'] = df_tipo_cliente_vf['rut'].astype('int')
df_tipo_cliente_vf.to_parquet(r'C:\Users\dponce\Documents\Trabajo\Panel de clientes\20220510_data_tipo_cliente_all.gzip', engine='pyarrow')
clv.to_excel(r'C:\Users\dponce\Documents\Trabajo\Panel de clientes\20220510_data_clv_cadena.xlsx')

