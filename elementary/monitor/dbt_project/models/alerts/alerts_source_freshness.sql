{{
  config(
    materialized = 'incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key = 'alert_id',
    on_schema_change = 'append_new_columns'
  )
}}


{% set alerts_source_freshness_relation = adapter.get_relation(this.database, this.schema, 'alerts_dbt_source_freshness') %}
{% if alerts_source_freshness_relation %}
    select
      alert_id,
      max_loaded_at,
      snapshotted_at,
      {{ elementary.edr_cast_as_timestamp("detected_at") }} as detected_at,
      max_loaded_at_time_ago_in_s,
      status,
      error,
      unique_id,
      database_name,
      schema_name,
      source_name,
      identifier,
      freshness_error_after,
      freshness_warn_after,
      freshness_filter,
      tags,
      meta,
      owner,
      package_name,
      path,
      false as alert_sent,  {# backwards compatibility #}
      'pending' as suppression_status,
      {{ elementary.edr_cast_as_string('NULL') }} as sent_at
    from {{ alerts_source_freshness_relation }}
    {% if is_incremental() %}
        {{ get_new_alerts_where_clause(this) }}
    {% endif %}
{% else %}
    {{ empty_alerts_source_freshness() }}
{% endif %}
