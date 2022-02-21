query_ecommerce = """ select 
                        tienda,
                        email,
                        order_id as orden,
                        fecha_completa as fecha,
                        sum(cantidad_venta) as unidades,
                        sum(total_value_orden) as monto_total,
                        sum(coupon_value_orden) as descuento_total,
                        1 as ecom
                    from analitica.reporte_ventas_consolidado
                    where fecha_completa between '{}' and '{}' 
                    and total_value_orden > 100
                    group by 1,2,3,4 """

query_brick_mortar =  """
                        select
                            v.cadena_lv,
                            v.tienda_lv,
                            v.nlocal_df,
                            v.agno,
                            v.mes,
                            v.date_df as fecha,
                            b.prm_email,
                            b.prm_rut,
                            b.foli_docu as boleta,
                            v.codpro_df as cod_prod,
                            sum(v.venta_neta) as venta_neta
                        from analitica.seleccion_ventas_oficiales as v
                        left join log_cliente_boleta as b
                        on v.nlocal_df = b.hed_local
                            and v.numfac_df = b.foli_docu
                        where v.date_df between '{}' and '{}'
                            and v.cadena_lv not in  ('ECOMMERCE', 'CD ECOMMERCE')
                            and v.venta_neta > 0
                        group by 1,2,3,4,5,6,7,8,9,10 """