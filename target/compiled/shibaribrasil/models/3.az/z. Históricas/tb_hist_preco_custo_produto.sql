

with cte_base as (
    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,ds_variacao
          ,vl_custo_cadastro
          ,vl_custo_ultima_compra
          ,vl_preco_venda
          ,vl_preco_venda_por
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,fg_produto_base_composicao
          ,fg_produto_composicao
          ,dt_ultima_compra
          ,date(dbt_valid_from, "America/Sao_Paulo")                                                        as dt_ini_vigencia
          ,time(dbt_valid_from, "America/Sao_Paulo")                                                        as hr_ini_vigencia
          ,coalesce(date(dbt_valid_to, "America/Sao_Paulo"), date(dbt_updated_at, "America/Sao_Paulo"))     as dt_fim_vigencia
          ,coalesce(time(dbt_valid_to, "America/Sao_Paulo"), time(dbt_updated_at, "America/Sao_Paulo"))     as hr_fim_vigencia
          ,datetime(dbt_updated_at, "America/Sao_Paulo")                                                    as dt_ultima_atualizacao
     from `igneous-sandbox-381622`.`snapshots`.`snapshot_preco_custo_produto`
    -- where concat(date(dbt_valid_from, "America/Sao_Paulo"), ' ', time(dbt_valid_from, "America/Sao_Paulo")) not in ('2025-07-16 16:57:12.010830') --reprocessamento para incremento de coluna
)

, cte_snapshot as (
    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,ds_variacao
          ,coalesce(nullif(vl_custo_ultima_compra,0), vl_custo_cadastro) vl_custo_final
          ,coalesce(nullif(vl_preco_venda_por,0), vl_preco_venda)        vl_preco_final
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,fg_produto_base_composicao
          ,fg_produto_composicao
          ,dt_ultima_compra
          ,dt_ini_vigencia
          ,hr_ini_vigencia
          ,dt_fim_vigencia
          ,hr_fim_vigencia
          ,dt_ultima_atualizacao
          ,row_number() over (partition by cd_produto_bling order by dt_ini_vigencia desc, hr_ini_vigencia desc) as nr_linha
     from cte_base
)

, cte_produtos_mudanca as (
  select cd_produto_bling, count(distinct nr_linha) 
    from cte_snapshot
  group by cd_produto_bling
  having count(distinct nr_linha) > 1
)

    select a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.ds_variacao
          ,coalesce(a.vl_custo_final, 0)         as vl_custo_final
          ,coalesce(a.vl_preco_final, 0)         as vl_preco_final
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.fg_produto_base_composicao
          ,a.fg_produto_composicao
          ,a.dt_ultima_compra
          ,a.dt_ini_vigencia
          ,a.hr_ini_vigencia
          ,a.dt_fim_vigencia
          ,a.hr_fim_vigencia
          ,a.dt_ultima_atualizacao
          ,a.nr_linha
          ,if(b.cd_produto_bling is not null, true, false)                                               as fg_alteracao
          ,lead(a.vl_custo_final)         over (partition by a.cd_produto_bling order by a.nr_linha asc) as vl_custo_final_anterior
          ,lead(a.vl_preco_final)         over (partition by a.cd_produto_bling order by a.nr_linha asc) as vl_preco_final_anterior
     from cte_snapshot         as a
left join cte_produtos_mudanca as b
       on a.cd_produto_bling = b.cd_produto_bling
 order by nm_produto
         ,ds_variacao