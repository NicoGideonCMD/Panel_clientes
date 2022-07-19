import datetime as dt

today = dt.date.today().strftime("%Y-%m-%d %H:%M:%S")
yesterday = dt.date.today() - dt.timedelta(days=9)
previous_week = dt.date.today() - dt.timedelta(days=374)

dict_ecommerce = {'fecha_inicio_ecom':previous_week.strftime("%Y-%m-%d %H:%M:%S"), # desde hace 374 días
                  'fecha_fin_ecom':yesterday.strftime("%Y-%m-%d %H:%M:%S")} # hasta hace 9 días atras

dict_bm = {'fecha_inicio_bm':previous_week.strftime("%Y-%m-%d %H:%M:%S"),
           'fecha_fin_bm':yesterday.strftime("%Y-%m-%d %H:%M:%S")}


