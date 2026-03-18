{% snapshot snapshot_status_pedido %}

{{
  config(
    target_schema='snapshots',
    unique_key='cd_codigo_interno',
    strategy='check',
    check_cols=['ds_status_pedido'], 
    invalidate_hard_deletes=True
  )
}}

    select cd_codigo_interno 
          ,cd_pedido         
          ,dt_pedido         
          ,cd_status_pedido  
          ,ds_status_pedido
     from {{ ref('stg_pedido') }}

{% endsnapshot %}