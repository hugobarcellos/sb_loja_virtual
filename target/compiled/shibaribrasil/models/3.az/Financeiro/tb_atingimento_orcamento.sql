

with cte_contas as (
  select date_trunc(cast(dt_vencimento as date), month) dt_prim_dia_mes
        -- ,cast(dt_vencimento as date)                    dt_data
        ,ds_categoria                                   ds_categoria
        -- ,coalesce(ds_subcategoria, ds_categoria)        ds_subcategoria
        -- ,ds_descricao                                   ds_descricao
        ,sum(vl_valor)                                  vl_valor
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_contas_pagar`
   where dt_vencimento >= '2026-03-01'
group by 1,2
)

, cte_orcamento as (
  select dt_prim_dia_mes
        ,ds_categoria_despesa
        ,vl_orcado
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_orcamento`
)

   select a.*
         ,coalesce(vl_orcado,0) vl_orcado
     from cte_contas as a
left join cte_orcamento as b
       on a.dt_prim_dia_mes = b.dt_prim_dia_mes
      and a.ds_categoria = b.ds_categoria_despesa