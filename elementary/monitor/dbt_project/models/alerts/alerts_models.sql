{{
  config(
    materialized = 'incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key = 'alert_id',
    on_schema_change = 'append_new_columns'
  )
}}


{% set error_models_relation = adapter.get_relation(this.database, this.schema, 'alerts_dbt_models') %}
{% if error_models_relation %}
    select
      alert_id,
      unique_id,
      {{ elementary.edr_cast_as_timestamp('detected_at') }} as detected_at,
      database_name,
      materialization,
      path,
      original_path,
      schema_name,
      message,
      owners,
      tags,
      alias,
      status,
      full_refresh,
      false as alert_sent,  {# backwards compatibility #}
      'pending' as suppression_status,
      {{ elementary.edr_cast_as_string('NULL') }} as sent_at
    from {{ error_models_relation }}
    {% if is_incremental() %}
        {{ elementary_cli.get_new_alerts_where_clause(this) }}
    {% endif %}
{% else %}
    {{ elementary_cli.empty_alerts_models() }}
{% endif %}
