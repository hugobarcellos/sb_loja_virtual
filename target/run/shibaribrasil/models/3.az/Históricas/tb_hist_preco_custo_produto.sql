
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_hist_preco_custo_produto`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

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
          ,date(dbt_valid_from, "America/Sao_Paulo")     as dt_ini_vigencia
          ,time(dbt_valid_from, "America/Sao_Paulo")     as hr_ini_vigencia
          ,date(dbt_valid_to, "America/Sao_Paulo")       as dt_fim_vigencia
          ,time(dbt_valid_to, "America/Sao_Paulo")       as hr_fim_vigencia
          ,datetime(dbt_updated_at, "America/Sao_Paulo") as dt_ultima_atualizacao
          ,row_number() over (partition by cd_produto_bling order by dbt_valid_to asc) as nr_linha
     from `igneous-sandbox-381622`.`snapshots`.`snapshot_preco_custo_produto`
)

, cte_produtos_mudanca as (
  select * 
    from cte_snapshot
   where nr_linha > 1
)

    select a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.ds_variacao
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.dt_ini_vigencia
          ,a.hr_ini_vigencia
          ,a.dt_fim_vigencia
          ,a.hr_fim_vigencia
          ,a.dt_ultima_atualizacao
          ,a.nr_linha
          ,if(b.cd_produto_bling is not null, true, false) fg_alteracao
     from cte_snapshot as a
left join cte_produtos_mudanca as b
       on a.cd_produto_bling = b.cd_produto_bling
 order by nm_produto
         ,ds_variacao
    );
  