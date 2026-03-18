
      merge into `igneous-sandbox-381622`.`snapshots`.`snapshot_status_pedido` as DBT_INTERNAL_DEST
    using `igneous-sandbox-381622`.`snapshots`.`snapshot_status_pedido__dbt_tmp` as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert (`cd_codigo_interno`, `cd_pedido`, `dt_pedido`, `cd_status_pedido`, `ds_status_pedido`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`)
        values (`cd_codigo_interno`, `cd_pedido`, `dt_pedido`, `cd_status_pedido`, `ds_status_pedido`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`)


  