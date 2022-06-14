query_ecommerce = """ select 
                        tienda,
                        email,
                        case 
                            when (rut like '%k' or rut like '%K') then cast(upper(substring(trim(rut) , 1, length(trim(rut))-1 ))as int)
                            when len(rut) = 9 then cast(left(rut, len(rut) - 1)as int)
                            else cast(rut as int)
                        end as rut,
                        order_id as orden,
                        fecha_completa as fecha,
                        sum(cantidad_venta) as unidades,
                        sum(total_value_orden) as monto_total,
                        sum(coupon_value_orden) as descuento_total,
                        1 as ecom
                    from analitica.reporte_ventas_consolidado
                    where fecha_completa between '{}' and '{}' 
                    and total_value_orden > 100
                    group by 1,2,3,4,5"""

query_brick_mortar =  """
                        select
                            v.cadena_lv,
                            v.tienda_lv,
                            v.nlocal_df,
                            v.agno,
                            v.mes,
                            v.date_df as fecha,
                            b.prm_email,
                            cast(upper(substring(trim(b.prm_rut) , 1, length(trim(b.prm_rut))-1 ))as int) as prm_rut,
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

query_ecom_all = """ 
                    select
                        tienda,
                        email,
                        case
                            when (rut like '%k' or rut like '%K')
                                then cast(upper(substring(trim(rut), 1, length(trim(rut)) - 1)) as int)
                            when len(rut) = 9 then cast(left(rut, len(rut) - 1) as int)
                            else cast(rut as int)
                            end           as rut,
                        order_id  as orden,
                        fecha_completa::date    as fecha,
                        cod_interno_forus::int as cod_prod,
                        sum(total_value_orden) as venta_neta,
                        'ecommerce'       as canal
         from analitica.reporte_ventas_consolidado
         where fecha_completa between '{}' and '{}'
           and total_value_orden > 100
        group by 1,2,3,4,5,6,8 """

query_bm_all = """ 
                     select v.cadena_lv,
                            b.prm_email,
                            b.prm_rut,
                            b.foli_docu  as boleta,
                            v.date_df    as fecha,
                            v.codpro_df  as cod_prod,
                            v.venta_neta as venta_neta,
                            'B&M'        as canal
                     from analitica.seleccion_ventas_oficiales as v
                              left join log_cliente_boleta as b
                                        on v.nlocal_df = b.hed_local
                                            and v.numfac_df = b.foli_docu
                     where v.date_df between '{}' and '{}'
                       and v.cadena_lv not in ('ECOMMERCE', 'CD ECOMMERCE')
                       and v.venta_neta > 0"""

query_segmentos = """           
                     select
                         lista_suscripcion,
                         email_cliente,
                         fecha_primera_compra,
                         fecha_ultima_compra,
                         nombre_cluster_marca
                     from middleware.segmentaciones_hubspot
                     where fecha_primera_compra >= '{}'
                     and fecha_ultima_compra < '{}'"""