### Scripts ###
from DataLoader import loader_data
from Help_Dict import dict_connection_aws, dict_query_ventas, dict_data_prep, dict_marcas_sitios
from Help_Function import function
from DataPrep import Data_Preparation
from Querys import querys

### Libs ###
import pandas as pd
import datetime
import warnings
import argparse

pd.set_option('display.max_columns',None)
warnings.filterwarnings("ignore")

####### 1.- Extract Data to Redshift ###########
print('Inicio de la extraccióm de data')
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
                    choices=['BM', 'ECOM'],
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

data_loader.close_connection()
print(df_data.fecha.min(), df_data.fecha.max())
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

tfin_rfmt = datetime.datetime.now() - tinicio_rfmt
print('el proceso del RFMT de datos se demoro {}'.format(tfin_prepdatos))

####### 4.- Apply Classification Model ###########
print('Inicio de la clasificación de clientes')

tinicio_class = datetime.datetime.now()

if execution_mode == 'BM':
    df_percentile = function.percentile25_75(df_tipo_cliente_rfmt, 'cadena', 'year', 'month', 'monetary')
    df_tipo_cliente_percentile = pd.merge(df_tipo_cliente_rfmt, df_percentile, how='left', on=['cadena', 'year', 'month']).fillna(0)

elif execution_mode == 'ECOM':
    df_percentile = function.percentile25_75(df_tipo_cliente_rfmt, 'sitio', 'year', 'month', 'monetary')
    df_tipo_cliente_percentile = pd.merge(df_tipo_cliente_rfmt, df_percentile, how='left', on=['sitio', 'year', 'month']).fillna(0)

df_tipo_cliente_percentile['classification'] = df_tipo_cliente_percentile.apply(lambda row: function.class_monetary(int(row['monetary']),
                                                                                                                    int(row['p75_monetary']),
                                                                                                                    int(row['p50_monetary']),
                                                                                                                    int(row['p25_monetary']),
                                                                                                                    int(row['upper_whisker']),
                                                                                                                    int(row['lower_whisker'])), axis=1)

df_tipo_cliente_vf = df_tipo_cliente_percentile.drop(columns={'p75_monetary', 'p25_monetary', 'upper_whisker', 'lower_whisker', 'iqr'})

tfin_class = datetime.datetime.now()
print('el proceso del Classification Model finalizo a las {}'.format(tfin_class))

