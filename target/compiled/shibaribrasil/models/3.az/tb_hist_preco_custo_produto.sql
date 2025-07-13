

with cte_snapshot as (
    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,ds_variacao
          ,vl_custo_compra
          ,vl_custo_total
          ,vl_preco_venda
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,datetime(dbt_updated_at, "America/Sao_Paulo") as dt_ultima_atualizacao
          ,datetime(dbt_valid_from, "America/Sao_Paulo") as dt_ini_vigencia
          ,datetime(dbt_valid_to, "America/Sao_Paulo")   as dt_fim_vigencia
     from `igneous-sandbox-381622`.`snapshots`.`snapshot_preco_custo_produto`
)

  select *
    from cte_snapshot
order by nm_produto
        ,ds_variacao