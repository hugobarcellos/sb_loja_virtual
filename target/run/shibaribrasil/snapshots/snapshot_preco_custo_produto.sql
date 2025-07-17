
      merge into `igneous-sandbox-381622`.`snapshots`.`snapshot_preco_custo_produto` as DBT_INTERNAL_DEST
    using `igneous-sandbox-381622`.`snapshots`.`snapshot_preco_custo_produto__dbt_tmp` as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert (`cd_produto_bling`, `cd_produto`, `cd_codigo_barras`, `nm_produto`, `ds_variacao`, `vl_custo_compra`, `vl_custo_total`, `vl_preco_venda`, `vl_preco_venda_por`, `vl_alteracao_preco`, `ds_subcategoria`, `ds_categoria`, `ds_classificacao_produto`, `ds_origem_produto`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`)
        values (`cd_produto_bling`, `cd_produto`, `cd_codigo_barras`, `nm_produto`, `ds_variacao`, `vl_custo_compra`, `vl_custo_total`, `vl_preco_venda`, `vl_preco_venda_por`, `vl_alteracao_preco`, `ds_subcategoria`, `ds_categoria`, `ds_classificacao_produto`, `ds_origem_produto`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`)


  