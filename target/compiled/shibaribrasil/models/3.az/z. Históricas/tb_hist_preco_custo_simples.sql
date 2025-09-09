

with cte_produto as (
  select cd_produto_bling
        ,cd_produto
        ,nm_produto
        ,ds_variacao
        ,vl_custo_cadastro
        ,vl_custo_ultima_compra
        ,coalesce(nullif(vl_custo_ultima_compra, 0), vl_custo_cadastro) vl_custo_final
        ,vl_preco_venda
        ,vl_preco_venda_por
        ,coalesce(nullif(vl_preco_venda_por, 0), vl_preco_venda) vl_preco_final
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_preco_produto`
)

, cte_tempo as (
  select distinct
         dt_prim_dia_mes
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_tempo`
   where dt_data >= '2024-01-01'
     and dt_data <= current_date
)

, cte_base as (
  select dt_prim_dia_mes
        ,cd_produto_bling
        ,cd_produto
        ,nm_produto
        ,ds_variacao
        ,vl_custo_cadastro
        ,vl_custo_ultima_compra
        ,vl_custo_final
        ,vl_preco_venda
        ,vl_preco_venda_por
        ,vl_preco_final
    from cte_produto
        ,cte_tempo
)

select *
  from cte_base

  -- 👇 só atualiza mês vigente
  where dt_prim_dia_mes = date_trunc(current_date, month)
