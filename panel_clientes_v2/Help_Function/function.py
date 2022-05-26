import pandas as pd
import numpy as np
import psycopg2

from datetime import date
from dateutil.relativedelta import relativedelta

def connection_aws(dbname, host, port, user, passw):
    con = psycopg2.connect(dbname= dbname,
                            host=host,
                            port=port,
                            user=user,
                            password=passw)
    return con

def rename_columns(df, dict):
    df_new = df.rename(columns=dict)
    return df_new


def previous_purchase(df, col1, col2, col3, col4, col5, col6, col0):
    df_sort = df.sort_values(by=[col2, col3])
    df_sort['fecha_compra_anterior_1'] = df_sort.groupby([col1, col2, col0])[col3].shift(periods=1)
    df_sort['fecha_compra_anterior_2'] = df_sort.groupby([col1, col2, col0])[col3].shift(periods=2)
    df_sort['fecha_compra_anterior'] = df_sort.apply(
        lambda row: row[col3] if pd.isna(row['fecha_compra_anterior_1']) == True
        else row['fecha_compra_anterior_1'], axis=1)
    df_sort['fecha_compra_pre_anterior'] = df_sort.apply(
        lambda row: row[col3] if pd.isna(row['fecha_compra_anterior_2']) == True
        else row['fecha_compra_anterior_2'], axis=1)

    df_sort['monto_compra_anterior'] = df_sort.groupby([col1, col2, col0])[col4].shift(periods=1).fillna(0)
    df_sort['unidades_compra_anterior'] = df_sort.groupby([col1, col2, col0])[col5].shift(periods=1).fillna(0)

    df_sort['fecha_compra_anterior'] = pd.to_datetime(df_sort['fecha_compra_anterior'], format='%Y-%m-%d')
    df_sort['fecha_compra_pre_anterior'] = pd.to_datetime(df_sort['fecha_compra_pre_anterior'], format='%Y-%m-%d')
    df_sort[col3] = pd.to_datetime(df_sort[col3], format='%Y-%m-%d')

    df_sort['anio_actual'] = df_sort[col3].dt.year
    df_sort['anio_compra_anterior'] = df_sort['fecha_compra_anterior'].dt.year

    df_sort['mes_actual'] = df_sort[col3].dt.month
    df_sort['mes_compra_anterior'] = df_sort['fecha_compra_anterior'].dt.month

    col_sorted = [col1, col2, col0, col3, 'fecha_compra_anterior', 'fecha_compra_pre_anterior',col4, 'anio_actual',
                  'anio_compra_anterior', 'mes_actual','mes_compra_anterior', 'monto_compra_anterior', col5,
                  'unidades_compra_anterior', col6]

    return df_sort[col_sorted].reset_index(drop=True)


def previous_purchase_clv(df, col1, col2, col3):
    df_sort = df.sort_values(by=[col2, col3])

    df_sort['fecha_compra_anterior_1'] = df_sort.groupby([col1, col2])[col3].shift(periods=1)

    df_sort['fecha_compra_anterior'] = df_sort.apply(
        lambda row: row[col3] if pd.isna(row['fecha_compra_anterior_1']) == True
        else row['fecha_compra_anterior_1'], axis=1)

    df_sort['fecha_compra_anterior'] = pd.to_datetime(df_sort['fecha_compra_anterior'], format='%Y-%m-%d')
    df_sort[col3] = pd.to_datetime(df_sort[col3], format='%Y-%m-%d')

    return df_sort


def tipo_cliente(col1, col2, col3, col4, col5, col6, meses, col8):
    tipo_cliente = ''

    if ((col3 == col4) & (col5 == col6)& (col8.month == col1.month)):
        tipo_cliente = 'cliente_nuevo'

    elif ((col8 < col1) or ((col1 <= (col2 + pd.DateOffset(months=meses))) &
          (aniomes((col2 + pd.DateOffset(months=meses)).year, (col2 + pd.DateOffset(months=meses)).month) >= aniomes(col5, col3)) &
          (aniomes(col5, col3) > aniomes(col6, col4)))):
        tipo_cliente = 'cliente_recurrente'

    elif col1 > (col2 + pd.DateOffset(months=meses)):
        tipo_cliente = 'cliente_reactivado'

    else:
        tipo_cliente = 'otros'

    return tipo_cliente


def activo_perdido(df, cadena, tienda ,email, fecha, dias):
    now = pd.to_datetime(date.today())
    df_max_fecha = df.groupby(['cadena', 'email']).agg({fecha:'max'}).reset_index().rename(columns={fecha:'fecha_max_compra'})
    df_max_fecha['activo_perdido'] = df_max_fecha.apply(lambda row: 'perdido' if (now - row['fecha_max_compra']).dt.days > dias  ## multiplicar los dias por n meses
                                                        else 'activo', axis=1)
    return df_max_fecha


def aniomes(anio, mes):
    anio_str = str(anio)
    mes_str = str(mes)
    aniomes = ''

    if len(mes_str) == 1:
        aniomes = anio_str + str(0) + mes_str
    else:
        aniomes = anio_str + mes_str
    return aniomes

def dest_email(email):
    num_character = email.find('@')
    dest_mail = email[0:num_character]
    return dest_mail

def mail_forus(email):
    num_character = email.find('@')
    mail = email[num_character+1:]
    return mail


def class_mail(email, list_email, forus_email, trash_mail, trash_mailpag):
    if ((dest_email(email) in list_email) or (mail_forus(email) in forus_email) or (
            (dest_email(email) in list_email) & (mail_forus(email) in forus_email))):
        tipo_correo = 'correo_forus'

    elif email == 'sin_informacion':
        tipo_correo = 'sin_correo'

    elif ((dest_email(email) in trash_mail) or (mail_forus(email) in trash_mailpag)):
        tipo_correo = 'correo_no_valido'

    else:
        tipo_correo = 'correo_cliente'

    return tipo_correo


def emaiL_classification(df, email, list_email, forus_email, trash_mail, trash_mailpag):
    df['tipo_correo'] = df.apply(lambda row: class_mail(row[email], list_email, forus_email, trash_mail, trash_mailpag),
                                 axis=1)
    return df

def data_prep_bm(df, email, rut, boleta):
    df_clean = df
    df_clean[email] = df_clean[email].fillna(value=np.nan)
    df_clean[email] = df_clean[email].fillna('sin_informacion')
    df_clean[rut] = df_clean[rut].fillna('sin_informacion')
    df_clean[boleta] = df_clean[boleta].fillna('999-9-99999999-9999')
    return df_clean

def data_prep_ecom(df, email, monto_total, descuento_total):
    df_clean = df
    df_clean[email] = df_clean[email].fillna('sin_informacion')
    df_clean[monto_total] = df_clean[monto_total].fillna(0)
    df_clean[descuento_total] = df_clean[descuento_total].fillna(0)
    return df_clean

def data_prep_all(df, email, venta_neta, rut, boleta, tienda):
    df_clean = df
    df_clean[email] = df_clean[email].fillna('sin_informacion')
    df_clean[venta_neta] = df_clean[venta_neta].fillna(0)
    df_clean[rut] = df_clean[rut].fillna('sin_informacion')
    df_clean[boleta] = df_clean[boleta].fillna('sin_informacion')
    df_clean[tienda] = df_clean[tienda].str.lower()
    return df_clean

def monetary(df, col0, col1, col2, col5):
    # col 0 is cadena col1 is the email's column and the col2 is the amount's column col3 is rut
    df_monetary = df.groupby([col0, col1, col5]).agg({col2:'sum'}).reset_index().rename(columns={col2:'monetary'})
    return df_monetary

def recency(df, col0, col1, col2, col5, now_date):
    # col1 is the email's column and col2 is the date's column col3 is rut
    df_max_date = df.groupby([col0, col1, col5]).agg({col2:np.max}).reset_index()
    df_max_date['diferencia'] = now_date - pd.to_datetime(df_max_date[col2])
    df_max_date['recency'] = df_max_date['diferencia'].dt.days
    df_recency = df_max_date[[col0, col1,col5,'recency']]
    return df_recency

def tenure(df, col0, col1, col2, col5, now_date):
    # col1 is the column email's and col2 is the column's date col3 is rut
    df_min_date = df.groupby([col0, col1, col5]).agg({col2:np.min}).reset_index()
    df_min_date['diferencia'] = now_date - pd.to_datetime(df_min_date[col2])
    df_min_date['tenure'] = df_min_date['diferencia'].dt.days
    df_tenure = df_min_date[[col0, col1, col5, 'tenure']]
    return df_tenure

def frequency(df, col0, col1, col2, col5):
   # col1 is the email's column and col2 is the order id's column col3 is rut
    df['email2'] = df[col1]
    df_freq = df.groupby([col0, col1, col5])[col2].nunique().reset_index()
    df_freq = df_freq.rename(columns={col2:'frequency'})
    return df_freq


def df_RFMT(df, col0, col1, col2, col3, col4, col5):
    # col0 is cadena's columns
    # col1 is the email's columns
    # col2 is the amaount's columns
    # col3 is the date's columns
    # col4 is the order_id's columns
    # col5 is rut

    today = pd.to_datetime(date.today())

    df_monetary = monetary(df, col0, col1, col2, col5)
    df_recency = recency(df, col0, col1, col3, col5, today)
    df_tenure = tenure(df, col0, col1, col3, col5, today)
    df_frequency = frequency(df, col0, col1, col4, col5)

    df_rfmt1 = pd.merge(df_recency, df_frequency, how='left', on=[col0, col1, col5])
    df_rfmt2 = pd.merge(df_monetary, df_tenure, how='left', on=[col0, col1, col5])
    df_rfmt3 = pd.merge(df_rfmt1, df_rfmt2, how='left', on=[col0, col1, col5])

    return df_rfmt3

def rfmt(df, mode):
    if mode == 'BM':

        columns_agg = ['cadena_lv', 'prm_email', 'prm_rut', 'agno','mes','fecha','boleta','tipo_correo']

        df_agg = df.groupby(columns_agg).agg({'venta_neta':'sum'}).reset_index().rename(columns={'nombre_cadena':'cadena', 'prm_email':'email', 'prm_rut':'rut', 'anyo':'anio', 'ventasiva':'monto_total_dia'})
        df_rfmt = df_RFMT(df_agg, 'cadena_lv', 'email', 'venta_neta', 'fecha', 'boleta', 'rut').sort_values(by='monetary')
        df_rfmt_clean = df_rfmt[(df_rfmt.email != 'sin_informacion')].rename(columns={'cadena_lv':'cadena', 'prm_rut':'rut-sin-dv'})

    elif mode == 'ECOM':

        df['fecha2'] = pd.to_datetime(df['fecha']).dt.date
        df['agno'] = df['fecha'].dt.year
        df['mes'] = df['fecha'].dt.month

        columns_agg = ['tienda', 'email','rut','agno', 'mes', 'fecha2', 'orden', 'tipo_correo']

        df_agg = df.groupby(columns_agg).agg({'monto_total': 'sum'}).reset_index().rename(columns={'agno': 'anio','monto_total': 'monto_total_dia', 'fecha2':'fecha', 'tienda':'sitio'})
        df_rfmt = df_RFMT(df_agg, 'sitio', 'email', 'monto_total_dia', 'fecha', 'orden', 'rut').sort_values(by='monetary')
        df_rfmt_clean = df_rfmt[(df_rfmt.email != 'sin_informacion')]

    elif mode == 'ALL':

        df['fecha2'] = pd.to_datetime(df['fecha']).dt.date
        df['agno'] = pd.to_datetime(df['fecha']).dt.year
        df['mes'] = pd.to_datetime(df['fecha']).dt.month

        columns_agg = ['tienda', 'email', 'rut', 'agno', 'mes', 'fecha2', 'boleta', 'tipo_correo']

        df_agg = df.groupby(columns_agg).agg({'venta_neta': 'sum'}).reset_index().rename(
            columns={'agno': 'anio', 'venta_neta': 'monto_total_dia', 'fecha2': 'fecha'})
        df_rfmt = df_RFMT(df_agg, 'tienda', 'email', 'monto_total_dia', 'fecha', 'boleta', 'rut').sort_values(
            by='monetary')
        df_rfmt_clean = df_rfmt[(df_rfmt.email != 'sin_informacion')]

    return df_rfmt_clean

def percentile25_75(df, cadena, agno, mes, monetary):
    df_clean_zeros = df[df[monetary]>0]
    df_p25 = df_clean_zeros.groupby([cadena, agno, mes]).monetary.quantile(0.25).reset_index()
    df_p75 = df_clean_zeros.groupby([cadena, agno, mes]).monetary.quantile(0.75).reset_index()
    df_p50 = df_clean_zeros.groupby([cadena, agno, mes]).monetary.quantile(0.50).reset_index()
    df_25_75 = pd.merge(df_p25, df_p75, how='inner', on = [cadena, agno, mes]).rename(columns={'monetary_x':'p25_monetary', 'monetary_y':'p75_monetary'})
    df_75_50 = pd.merge(df_25_75, df_p50, how='inner', on=[cadena, agno, mes]).rename(columns={'monetary': 'p50_monetary'})
    df_75_50['iqr'] = df_75_50['p75_monetary'] - df_75_50['p25_monetary']
    df_75_50['upper_whisker'] = (1.5*df_75_50['iqr']) + df_75_50['p75_monetary']
    df_75_50['lower_whisker'] = (1.5*df_75_50['iqr']) - df_75_50['p25_monetary']
    return df_75_50.dropna()

def class_monetary(monetary, p75, p50 ,p25, upper_whisker, lower_whisker):
    classification = ''
    if (monetary > upper_whisker):
        classification = 'AA'
    elif ((monetary >= p75) & (monetary < upper_whisker)):
        classification = 'A'
    elif ((monetary >= p50) & (monetary < p75)):
        classification = 'B'
    elif ((monetary >= p25) & (monetary < p50)):
        classification = 'C'
    elif ((monetary >= lower_whisker) & (monetary < p25)):
        classification = 'D'
    elif (monetary < lower_whisker):
        classification = 'E'
    return classification

def dv(rut):
    try:
        len_rut = len(rut)
        rut_sdv = rut[0:len_rut-1]
    except:
        rut_sdv = 0
    return rut_sdv


def prep_clv(df_data):
    #### df_data debe venir filtrada con la funcion de email_clasification y tipo_correo == 'correo_cliente'
    #### Preparación DF
    df_data['agno'] = pd.DatetimeIndex(df_data['fecha']).year
    df_data['mes'] = pd.DatetimeIndex(df_data['fecha']).month
    df_agg = df_data.groupby(['cadena', 'agno', 'mes', 'fecha', 'email']).agg(
        {'monto_total': 'sum'}).reset_index()
    df_previous_pruchase = previous_purchase_clv(df_agg, 'cadena', 'email', 'fecha')  # FUNCION DE previous_purchase
    df_previous_pruchase_sort = df_previous_pruchase.sort_values(by=['cadena', 'email', 'fecha']).drop(
        columns={'fecha_compra_anterior_1'})
    df_previous_pruchase_sort['email_shift'] = df_previous_pruchase_sort.groupby('cadena')['email'].shift(periods=1)
    df_previous_pruchase_sort['cliente_mas_1_compra'] = df_previous_pruchase_sort.apply(
        lambda row: 1 if ((row['fecha'] != row['fecha_compra_anterior'])
                          & (row['email'] == row['email_shift']))
        else 0, axis=1)

    df_previous_pruchase_sort['id_cliente_cadena'] = df_previous_pruchase_sort['cadena'] + df_previous_pruchase_sort[
        'email']

    unique_mail = list(
        df_previous_pruchase_sort.id_cliente_cadena[df_previous_pruchase_sort.cliente_mas_1_compra >= 0].unique())

    columns_sort = ['cadena', 'email', 'agno', 'mes', 'fecha', 'fecha_compra_anterior', 'monto_total',
                    'id_cliente_cadena']
    df_clientes_recompra = df_previous_pruchase_sort[
        df_previous_pruchase_sort.id_cliente_cadena.isin(unique_mail)].drop(
        columns={'email_shift', 'cliente_mas_1_compra'})
    df_clientes_recompra_sort = df_clientes_recompra[columns_sort]

    df_clientes_recompra_sort['dias_entre_compras'] = (
                df_clientes_recompra_sort['fecha'] - df_clientes_recompra_sort['fecha_compra_anterior']).dt.days
    df_clientes_recompra_sort['num_compra'] = df_clientes_recompra_sort.groupby('id_cliente_cadena')['fecha'].rank(
        method='first')

    df_clientes_clean = df_clientes_recompra_sort[(df_clientes_recompra_sort['num_compra'] <= 20)]
    df_clientes_clean_export = df_clientes_clean.drop(columns=['id_cliente_cadena'])

    df_clientes_clean_agg = df_clientes_clean_export.groupby(['cadena', 'num_compra']).agg(
        {'email': 'count'  # solo deben entrar clientes cuyo tipo_correo sea valido
            , 'monto_total': 'mean'
            , 'dias_entre_compras': 'mean'})
    return df_clientes_clean_agg


def clv_generator(df_clientes_clean_agg, tasa):
    df_clientes_clean_agg['pct_clientes'] = df_clientes_clean_agg['email'] / df_clientes_clean_agg.groupby(level=[0])[
        'email'].transform('sum')
    df_clientes_clean_agg['tasa_retencion'] = df_clientes_clean_agg['email'] / df_clientes_clean_agg.groupby(level=[0])[
        'email'].transform('max')
    df_clientes_clean_agg['tasa_descuento'] = tasa
    df_clientes_clean_agg['dias_acum'] = df_clientes_clean_agg.groupby(level=[0])['dias_entre_compras'].cumsum()
    df_clientes_clean_agg['periodo_equivalente'] = df_clientes_clean_agg['dias_acum'] / 30
    df_clientes_clean_agg['CLV_spot'] = df_clientes_clean_agg['monto_total'] * df_clientes_clean_agg[
        'tasa_retencion'] / ((1 + df_clientes_clean_agg['tasa_descuento']) ** df_clientes_clean_agg[
        'periodo_equivalente'])

    return df_clientes_clean_agg

def get_clv(df_data, tasa=0.0064):
    df_clientes_clean_agg = prep_clv(df_data)
    df_clientes_clean_agg2 = clv_generator(df_clientes_clean_agg , tasa)
    df_clientes_clv = df_clientes_clean_agg2.groupby(level=[0])[['CLV_spot']].sum()
    return df_clientes_clv

def homo_tienda(row, dict_tienda):
    for key, value in dict_tienda.items():
        if row in value:
            return key




