
   
      -- generated script to merge partitions into `igneous-sandbox-381622`.`dbt_dw_az`.`tb_hist_preco_custo_simples`
      declare dbt_partitions_for_replacement array<date>;

      
      
       -- 1. create a temp table with model data
        
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_hist_preco_custo_simples__dbt_tmp`
      
    partition by date_trunc(dt_prim_dia_mes, month)
    

    OPTIONS(
      description="""""",
    
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      

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

    );
  
      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null
              array_agg(distinct date_trunc(dt_prim_dia_mes, month) IGNORE NULLS)
          from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_hist_preco_custo_simples__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `igneous-sandbox-381622`.`dbt_dw_az`.`tb_hist_preco_custo_simples` as DBT_INTERNAL_DEST
        using (
        select
        * from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_hist_preco_custo_simples__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date_trunc(DBT_INTERNAL_DEST.dt_prim_dia_mes, month) in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`dt_prim_dia_mes`, `cd_produto_bling`, `cd_produto`, `nm_produto`, `ds_variacao`, `vl_custo_cadastro`, `vl_custo_ultima_compra`, `vl_custo_final`, `vl_preco_venda`, `vl_preco_venda_por`, `vl_preco_final`)
    values
        (`dt_prim_dia_mes`, `cd_produto_bling`, `cd_produto`, `nm_produto`, `ds_variacao`, `vl_custo_cadastro`, `vl_custo_ultima_compra`, `vl_custo_final`, `vl_preco_venda`, `vl_preco_venda_por`, `vl_preco_final`)

;

      -- 4. clean up the temp table
      drop table if exists `igneous-sandbox-381622`.`dbt_dw_az`.`tb_hist_preco_custo_simples__dbt_tmp`

  


  

    