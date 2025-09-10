
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_estoque_produto_base`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_produto as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,coalesce(a.vl_custo_compra, a.vl_custo_total)              as vl_custo_total -- valores atuais do cadastro
          ,coalesce(nullif(a.vl_preco_venda_por,0), a.vl_preco_venda) as vl_preco_venda -- valores atuais do cadastro
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.dt_ultima_ingestao
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto`  as a   
)

, cte_venda_produto as (
   select a.cd_produto_bling                 as cd_produto_bling
         ,a.cd_produto                       as cd_produto
         ,a.cd_codigo_barras                 as cd_codigo_barras
         ,a.nm_produto                       as nm_produto
         ,a.nm_produto_completo              as nm_produto_completo
         ,a.ds_variacao                      as ds_variacao
         ,a.ds_subcategoria                  as ds_subcategoria
         ,a.ds_categoria                     as ds_categoria
         ,a.ds_classificacao_produto         as ds_classificacao_produto
         ,a.ds_origem_produto                as ds_origem_produto
         ,a.qt_pecas_mes_atual               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses              as qt_pecas_tres_meses 
         ,a.qt_pecas_seis_meses              as qt_pecas_seis_meses 
         ,a.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias
         ,a.ds_classificacao_abc             as ds_classificacao_abc
         ,a.vl_percentual_participacao       as vl_percentual_participacao
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_venda_produto_base` as a
)

, cte_tempo as (
    select dt_data
          ,dt_prim_dia_mes
      from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_tempo`
     where dt_data >= date_trunc(date_sub(current_date(), interval 6 month), month)
       and dt_data <= current_date
)

, cte_tempo_mes_atual as (
    select count(distinct dt_data) qt_dias_mes_atual
      from cte_tempo
     where dt_data >= date_trunc(date_sub(current_date(), interval 0 month), month)
       and dt_data <= current_date
)

, cte_tempo_mes_anterior as (
    select count(distinct dt_data) qt_dias_mes_anterior
      from cte_tempo
     where dt_data >= date_trunc(date_sub(current_date(), interval 1 month), month)
       and dt_data <  date_trunc(date_sub(current_date(), interval 0 month), month)
)

, cte_tempo_tres_meses as (
    select count(distinct dt_data) qt_dias_tres_meses
      from cte_tempo
     where dt_data >= date_trunc(date_sub(current_date(), interval 3 month), month)
       and dt_data <  date_trunc(date_sub(current_date(), interval 0 month), month)
)

, cte_base as (
   select a.cd_produto_bling                 as cd_produto_bling
         ,a.cd_produto                       as cd_produto
         ,a.cd_codigo_barras                 as cd_codigo_barras
         ,a.nm_produto                       as nm_produto
         ,a.nm_produto_completo              as nm_produto_completo
         ,a.ds_variacao                      as ds_variacao
         ,a.ds_subcategoria                  as ds_subcategoria
         ,a.ds_categoria                     as ds_categoria
         ,a.ds_classificacao_produto         as ds_classificacao_produto
         ,a.ds_origem_produto                as ds_origem_produto
         ,a.ds_classificacao_abc             as ds_classificacao_abc
         ,a.vl_percentual_participacao       as vl_percentual_participacao
         ,b.qt_estoque_minimo                as qt_estoque_minimo
         ,b.qt_estoque_atual                 as qt_estoque_atual
         ,b.vl_custo_total                   as vl_custo_total
         ,b.vl_preco_venda                   as vl_preco_venda
         ,a.qt_pecas_mes_atual               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses              as qt_pecas_tres_meses 
         ,a.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias
         ,c.qt_dias_mes_atual                as qt_dias_mes_atual
         ,d.qt_dias_mes_anterior             as qt_dias_mes_anterior
         ,e.qt_dias_tres_meses               as qt_dias_tres_meses
         ,b.qt_lead_time                     as qt_lead_time
         ,b.qt_cobertura_desejada            as qt_cobertura_desejada
         ,b.nm_fornecedor                    as nm_fornecedor
         ,b.dt_ultima_ingestao               as dt_ultima_ingestao
     from cte_venda_produto        as a
         ,cte_tempo_mes_atual      as c
         ,cte_tempo_mes_anterior   as d
         ,cte_tempo_tres_meses     as e
left join cte_produto              as b 
       on a.cd_produto_bling = b.cd_produto_bling
)

, cte_compra_produto as (
    select cd_compra
          ,cd_produto_bling
          ,qt_item
          ,vl_item
          ,lk_produto_compra
          ,ds_status_compra
          ,nr_seq
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_agg_compra_produto`
)

, cte_produto_fabricado as (
    select cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,sum(vl_custo_compra_total) vl_custo_compra_total
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto_fabricado`
  group by cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
)

, cte_joins as (
   select a.cd_produto_bling                                 as cd_produto_bling
         ,a.cd_produto                                       as cd_produto
         ,a.cd_codigo_barras                                 as cd_codigo_barras
         ,a.nm_produto                                       as nm_produto
         ,a.nm_produto_completo                              as nm_produto_completo
         ,a.ds_variacao                                      as ds_variacao
         ,a.ds_subcategoria                                  as ds_subcategoria
         ,a.ds_categoria                                     as ds_categoria
         ,a.ds_classificacao_produto                         as ds_classificacao_produto
         ,a.ds_origem_produto                                as ds_origem_produto
         ,a.qt_lead_time                                     as qt_lead_time
         ,a.qt_cobertura_desejada                            as qt_cobertura_desejada
         ,a.ds_classificacao_abc                             as ds_classificacao_abc
         ,a.vl_percentual_participacao                       as vl_percentual_participacao
         ,if(c.cd_produto_bling  is null, false, true)       as fg_produto_fabricado
         ,a.qt_estoque_minimo                                as qt_estoque_minimo
         ,a.qt_estoque_atual                                 as qt_estoque_atual
         ,a.vl_custo_total                                   as vl_custo_total
         ,a.vl_preco_venda                                   as vl_preco_venda
         ,sum(b.qt_item)                                     as qt_item_compra_pendente
         ,max(b.vl_item)                                     as vl_item_compra_pendente
         ,a.qt_pecas_mes_atual                               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior                            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses                              as qt_pecas_tres_meses 
         ,a.qt_pecas_sessenta_dias                           as qt_pecas_sessenta_dias
         ,a.qt_dias_mes_atual                                as qt_dias_mes_atual
         ,a.qt_dias_mes_anterior                             as qt_dias_mes_anterior
         ,a.qt_dias_tres_meses                               as qt_dias_tres_meses
         ,a.nm_fornecedor                                    as nm_fornecedor
         ,d.lk_produto_compra                                as lk_produto_compra
         ,a.dt_ultima_ingestao                               as dt_ultima_ingestao
         ,datetime(current_timestamp(), "America/Sao_Paulo") as dt_ultima_atualizacao
     from cte_base               as a
left join cte_compra_produto     as b
       on a.cd_produto_bling = b.cd_produto_bling
      and b.ds_status_compra in ('EM ABERTO')
left join cte_compra_produto     as d
       on a.cd_produto_bling = d.cd_produto_bling
      and d.nr_seq = 1
left join cte_produto_fabricado  as c
       on a.cd_produto_bling = c.cd_produto_bling
 group by a.cd_produto_bling
         ,a.cd_produto
         ,a.cd_codigo_barras
         ,a.nm_produto
         ,a.nm_produto_completo
         ,a.ds_variacao
         ,a.ds_subcategoria
         ,a.ds_categoria
         ,a.ds_classificacao_produto
         ,a.ds_origem_produto
         ,a.qt_lead_time   
         ,a.qt_cobertura_desejada 
         ,a.ds_classificacao_abc
         ,a.vl_percentual_participacao
         ,c.cd_produto_bling
         ,a.qt_estoque_minimo
         ,a.qt_estoque_atual
         ,a.vl_custo_total
         ,a.vl_preco_venda
         ,a.qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses 
         ,a.qt_pecas_sessenta_dias
         ,a.qt_dias_mes_atual
         ,a.qt_dias_mes_anterior
         ,a.qt_dias_tres_meses
         ,a.nm_fornecedor
         ,d.lk_produto_compra
         ,a.dt_ultima_ingestao
         
)

, cte_cobertura_atual as (
   select cd_produto_bling
         ,cd_produto
         ,cd_codigo_barras
         ,nm_produto
         ,nm_produto_completo
         ,ds_variacao
         ,ds_subcategoria
         ,ds_categoria
         ,ds_classificacao_produto
         ,ds_origem_produto
         ,qt_lead_time
         ,qt_cobertura_desejada
         ,ds_classificacao_abc
         ,vl_percentual_participacao
         ,fg_produto_fabricado
         ,qt_estoque_minimo
         ,qt_estoque_atual
         ,coalesce(safe_divide(qt_estoque_atual, safe_divide(qt_pecas_sessenta_dias , 60)),0) qt_cobertura_atual
         ,vl_custo_total
         ,vl_preco_venda
         ,qt_item_compra_pendente
         ,vl_item_compra_pendente
         ,coalesce(safe_divide(qt_item_compra_pendente, safe_divide(qt_pecas_sessenta_dias , 60)),0) qt_compra_pendente_dia
         ,qt_pecas_mes_atual 
         ,qt_pecas_mes_anterior 
         ,qt_pecas_tres_meses 
         ,qt_pecas_sessenta_dias
         ,qt_dias_mes_atual
         ,qt_dias_mes_anterior
         ,qt_dias_tres_meses
         ,nm_fornecedor
         ,lk_produto_compra
         ,dt_ultima_ingestao
         ,dt_ultima_atualizacao
     from cte_joins
)

, ds_cobertura_total as (
  select cd_produto_bling
         ,cd_produto
         ,cd_codigo_barras
         ,nm_produto
         ,nm_produto_completo
         ,ds_variacao
         ,ds_subcategoria
         ,ds_categoria
         ,ds_classificacao_produto
         ,ds_origem_produto
         ,qt_lead_time
         ,qt_cobertura_desejada
         ,ds_classificacao_abc
         ,vl_percentual_participacao
         ,fg_produto_fabricado
         ,qt_estoque_minimo
         ,qt_estoque_atual
         ,qt_cobertura_atual
         ,coalesce(((qt_cobertura_atual + qt_compra_pendente_dia) - qt_lead_time),0) qt_cobertura_total
         ,vl_custo_total
         ,vl_preco_venda
         ,qt_item_compra_pendente
         ,vl_item_compra_pendente
         ,qt_compra_pendente_dia
         ,qt_pecas_mes_atual 
         ,qt_pecas_mes_anterior 
         ,qt_pecas_tres_meses 
         ,qt_pecas_sessenta_dias
         ,qt_dias_mes_atual
         ,qt_dias_mes_anterior
         ,qt_dias_tres_meses
         ,nm_fornecedor
         ,lk_produto_compra
         ,dt_ultima_ingestao
         ,dt_ultima_atualizacao
     from cte_cobertura_atual
)

, cte_classificacao_risco as (
   select cd_produto_bling
         ,cd_produto
         ,cd_codigo_barras
         ,nm_produto
         ,nm_produto_completo
         ,ds_variacao
         ,ds_subcategoria
         ,ds_categoria
         ,ds_classificacao_produto
         ,ds_origem_produto
         ,qt_lead_time
         ,qt_cobertura_desejada
         ,ds_classificacao_abc
         ,vl_percentual_participacao
         ,fg_produto_fabricado
         ,qt_estoque_minimo
         ,qt_estoque_atual
         ,qt_cobertura_atual
         ,qt_cobertura_total
         ,case
            when qt_pecas_sessenta_dias = 0 and qt_estoque_atual = 0
                then 'g. ⚫ Sem Histórico'
            when qt_pecas_sessenta_dias = 0 and qt_estoque_atual > 0
                then 'f. 💤 Encalhado'
            when qt_pecas_sessenta_dias > 0 and qt_estoque_atual = 0
                then 'a. 🆘 Estoque Rompido'
             when qt_estoque_atual > 0 and qt_cobertura_total < 30
                then 'b. 🛑 Urgente'
            when qt_estoque_atual > 0 and qt_estoque_atual <= qt_estoque_minimo and qt_cobertura_total < 45
                then 'c. ⚠️ Atenção'
            when qt_estoque_atual > 0 and qt_cobertura_total >= 45 and qt_cobertura_total <= 60
                then 'd. ✅ Estável'
            when qt_estoque_atual > 0 and qt_cobertura_total > 60
                then 'e. 💠 Sobreestoque'
            else 'h. Sem Classificação'
            end ds_classificacao_risco
         ,vl_custo_total
         ,vl_preco_venda
         ,qt_item_compra_pendente
         ,vl_item_compra_pendente
         ,qt_compra_pendente_dia
         ,qt_pecas_mes_atual 
         ,qt_pecas_mes_anterior 
         ,qt_pecas_tres_meses 
         ,qt_pecas_sessenta_dias
         ,qt_dias_mes_atual
         ,qt_dias_mes_anterior
         ,qt_dias_tres_meses
         ,nm_fornecedor
         ,lk_produto_compra
         ,dt_ultima_ingestao
         ,dt_ultima_atualizacao
     from ds_cobertura_total
)
  select *
    from cte_classificacao_risco
order by nm_produto
        ,ds_variacao
    );
  