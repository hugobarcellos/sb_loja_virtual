{{ config(
    tags = ['az', 'produto'],
    enabled = true
)}}

with cte_estoque as (
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
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.ds_tipo_estoque
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
      from {{ ref('tb_estoque') }}  as a  
     where a.fg_produto_composicao is not true
)

, cte_venda_produto as (
   select a.cd_produto_bling                 as cd_produto_bling
         ,a.cd_produto                       as cd_produto
         ,a.cd_codigo_barras                 as cd_codigo_barras
         ,a.nm_produto_completo              as nm_produto_completo
         ,a.qt_pecas_mes_atual               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses              as qt_pecas_tres_meses 
         ,a.qt_pecas_seis_meses              as qt_pecas_seis_meses 
         ,a.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias
         ,a.ds_classificacao_abc             as ds_classificacao_abc
         ,a.vl_percentual_participacao       as vl_percentual_participacao
     from {{ ref('tb_venda_produto_base') }} as a
)

, cte_tempo as (
    select dt_data
          ,dt_prim_dia_mes
      from {{ ref('stg_tempo') }}
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

, cte_venda_base as (
   select a.cd_produto_bling                 as cd_produto_bling
         ,a.cd_produto                       as cd_produto
         ,a.cd_codigo_barras                 as cd_codigo_barras
         ,a.nm_produto_completo              as nm_produto_completo
         ,a.ds_classificacao_abc             as ds_classificacao_abc
         ,a.vl_percentual_participacao       as vl_percentual_participacao
         ,a.qt_pecas_mes_atual               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses              as qt_pecas_tres_meses 
         ,a.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias
         ,c.qt_dias_mes_atual                as qt_dias_mes_atual
         ,d.qt_dias_mes_anterior             as qt_dias_mes_anterior
         ,e.qt_dias_tres_meses               as qt_dias_tres_meses
     from cte_venda_produto        as a
         ,cte_tempo_mes_atual      as c
         ,cte_tempo_mes_anterior   as d
         ,cte_tempo_tres_meses     as e
)

, cte_base as (
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
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.ds_tipo_estoque
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,b.ds_classificacao_abc             as ds_classificacao_abc
          ,b.vl_percentual_participacao       as vl_percentual_participacao
          ,b.qt_pecas_mes_atual               as qt_pecas_mes_atual 
          ,b.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
          ,b.qt_pecas_tres_meses              as qt_pecas_tres_meses 
          ,b.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias
          ,b.qt_dias_mes_atual                as qt_dias_mes_atual
          ,b.qt_dias_mes_anterior             as qt_dias_mes_anterior
          ,b.qt_dias_tres_meses               as qt_dias_tres_meses
      from cte_estoque    as a  
 left join cte_venda_base as b 
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
     from {{ ref('tb_agg_compra_produto') }}
)

, cte_produto_fabricado as (
    select cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,sum(vl_custo_compra_total) vl_custo_compra_total
      from {{ ref('tb_produto_fabricado') }}
  group by cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
)

, cte_joins as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.ds_tipo_estoque
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,sum(b.qt_item)                                     as qt_item_compra_pendente
          ,max(b.vl_item)                                     as vl_item_compra_pendente
          ,a.ds_classificacao_abc
          ,a.vl_percentual_participacao
          ,a.qt_pecas_mes_atual 
          ,a.qt_pecas_mes_anterior 
          ,a.qt_pecas_tres_meses 
          ,a.qt_pecas_sessenta_dias
          ,a.qt_dias_mes_atual
          ,a.qt_dias_mes_anterior
          ,a.qt_dias_tres_meses
          ,datetime(current_timestamp(), "America/Sao_Paulo") as dt_ultima_atualizacao
      from cte_base               as a  
 left join cte_compra_produto     as b
        on a.cd_produto_bling = b.cd_produto_bling
       and b.ds_status_compra in ('EM ABERTO')
  group by a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.ds_tipo_estoque
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.ds_classificacao_abc
          ,a.vl_percentual_participacao
          ,a.qt_pecas_mes_atual 
          ,a.qt_pecas_mes_anterior 
          ,a.qt_pecas_tres_meses 
          ,a.qt_pecas_sessenta_dias
          ,a.qt_dias_mes_atual
          ,a.qt_dias_mes_anterior
          ,a.qt_dias_tres_meses

)

, cte_cobertura_atual as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.ds_tipo_estoque
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,coalesce(safe_divide(qt_estoque_atual, safe_divide(qt_pecas_sessenta_dias , 60)),0) qt_cobertura_atual
          ,a.qt_item_compra_pendente
          ,a.vl_item_compra_pendente
          ,coalesce(safe_divide(qt_item_compra_pendente, safe_divide(qt_pecas_sessenta_dias , 60)),0) qt_compra_pendente_dia
          ,a.ds_classificacao_abc
          ,a.vl_percentual_participacao
          ,a.qt_pecas_mes_atual 
          ,a.qt_pecas_mes_anterior 
          ,a.qt_pecas_tres_meses 
          ,a.qt_pecas_sessenta_dias
          ,a.qt_dias_mes_atual
          ,a.qt_dias_mes_anterior
          ,a.qt_dias_tres_meses
          ,a.dt_ultima_atualizacao
      from cte_joins               as a  
)

, cte_cobertura_total as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.ds_tipo_estoque
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.qt_cobertura_atual
          ,coalesce(((qt_cobertura_atual + qt_compra_pendente_dia) - qt_lead_time),0) qt_cobertura_total
          ,a.qt_item_compra_pendente
          ,a.vl_item_compra_pendente
          ,a.qt_compra_pendente_dia
          ,a.ds_classificacao_abc
          ,a.vl_percentual_participacao
          ,a.qt_pecas_mes_atual 
          ,a.qt_pecas_mes_anterior 
          ,a.qt_pecas_tres_meses 
          ,a.qt_pecas_sessenta_dias
          ,a.qt_dias_mes_atual
          ,a.qt_dias_mes_anterior
          ,a.qt_dias_tres_meses
          ,a.dt_ultima_atualizacao
      from cte_cobertura_atual               as a  
)

, cte_classificacao_risco as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.ds_tipo_estoque
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.qt_cobertura_atual
          ,a.qt_cobertura_total
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
          ,a.qt_item_compra_pendente
          ,a.vl_item_compra_pendente
          ,a.qt_compra_pendente_dia
          ,a.ds_classificacao_abc
          ,a.vl_percentual_participacao
          ,a.qt_pecas_mes_atual 
          ,a.qt_pecas_mes_anterior 
          ,a.qt_pecas_tres_meses 
          ,a.qt_pecas_sessenta_dias
          ,a.qt_dias_mes_atual
          ,a.qt_dias_mes_anterior
          ,a.qt_dias_tres_meses
          ,a.dt_ultima_atualizacao
      from cte_cobertura_total               as a  
)

  select *
    from cte_classificacao_risco
order by nm_produto
        ,ds_variacao