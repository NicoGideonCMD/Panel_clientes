import datetime as dt

today = dt.date.today().strftime("%Y-%m-%d")
yesterday = dt.date.today() - dt.timedelta(days=9)
previous_week = dt.date.today() - dt.timedelta(days=374)

dict_ecommerce = {'fecha_inicio_ecom':previous_week.strftime("%Y-%m-%d"),
                  'fecha_fin_ecom':yesterday.strftime("%Y-%m-%d")}

dict_bm = {'fecha_inicio_bm':previous_week.strftime("%Y-%m-%d"),
           'fecha_fin_bm':yesterday.strftime("%Y-%m-%d")}


