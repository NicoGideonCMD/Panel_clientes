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
select  t.nombre_cadena as cadena_lv,
        t.nombre_tienda as tienda_lv,
        v.cod_tienda as nlocal_df,
        v.anyo as agno,
        v.mes::VARCHAR as mes,
        to_char(v.fecha_transaccion,'YYYY-MM-DD') as fecha,
        b.prm_email,
        cast(upper(substring(trim(b.prm_rut) , 1, length(trim(b.prm_rut))-1 ))as int) as prm_rut,        
        v.numero_impreso as boleta,        
        v.id_producto as cod_prod,
        sum(v.ventasiva) as venta_neta        
from public.ventas_bi v
left join public.log_cliente_boleta b on v.cod_tienda = b.hed_local and v.numero_impreso = b.foli_docu
left join analitica.maestro_tiendas t on v.cod_tienda = t.codigo_tienda
where v.fecha_transaccion between  '{}' and '{}'
      and v.ventasiva>100
      and v.cantidad>0      
      and v.anyo>=2015
      and v.forma_pago_ec='N'
      and t.nombre_cadena not in ('CD ECOMMERCE','ECOMMERCE')
      and v.clase <> 'DESPACHOS'
group by 1,2,3,4,5,6,7,8,9,10
 """

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
select  t.nombre_cadena as cadena_lv,
        b.prm_email,
        b.prm_rut,
        v.numero_impreso as boleta,
        to_char(v.fecha_transaccion,'YYYY-MM-DD') as fecha,
        v.id_producto as cod_prod,
        v.ventasiva as venta_neta,
        'B&M' as canal
from public.ventas_bi v
left join public.log_cliente_boleta b on v.cod_tienda = b.hed_local and v.numero_impreso = b.foli_docu
left join analitica.maestro_tiendas t on v.cod_tienda = t.codigo_tienda
where v.fecha_transaccion between '{}' and '{}'
      and v.ventasiva>100
      and v.cantidad>0      
      and v.anyo>=2015
      and v.forma_pago_ec='N'
      and t.nombre_cadena not in ('CD ECOMMERCE','ECOMMERCE')
      and v.clase <> 'DESPACHOS'
                     """

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
