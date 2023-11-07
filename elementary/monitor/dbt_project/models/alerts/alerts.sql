{{
  config(
    materialized = 'incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key = 'alert_id',
    on_schema_change = 'append_new_columns'
  )
}}

{% set anomaly_detection_relation = adapter.get_relation(this.database, this.schema, 'alerts_anomaly_detection') %}
{# Backwards compatibility support for a renamed model. #}
{% set data_monitoring_relation = adapter.get_relation(this.database, this.schema, 'alerts_data_monitoring') %}
{% set schema_changes_relation = adapter.get_relation(this.database, this.schema, 'alerts_schema_changes') %}

with failed_tests as (
    select * from {{ ref('elementary', 'alerts_dbt_tests') }}

    {% if schema_changes_relation %}
        union all
        select * from {{ schema_changes_relation }}
    {% endif %}

    {% if anomaly_detection_relation %}
        union all
        select * from {{ anomaly_detection_relation }}
    {% elif data_monitoring_relation %}
        union all
        select * from {{ data_monitoring_relation }}
    {% endif %}
)

select
    alert_id,
    data_issue_id,
    test_execution_id,
    test_unique_id,
    model_unique_id,
    {{ elementary.edr_cast_as_timestamp('detected_at') }} as detected_at,
    database_name,
    schema_name,
    table_name,
    column_name,
    alert_type,
    sub_type,
    alert_description,
    owners,
    tags,
    alert_results_query,
    other,
    test_name,
    test_short_name,
    test_params,
    severity,
    status,
    result_rows,
    false as alert_sent,  {# backwards compatibility #}
    'pending' as suppression_status,
    {{ elementary.edr_cast_as_string('NULL') }} as sent_at
from failed_tests

{%- if is_incremental() %}
    {{ elementary_cli.get_new_alerts_where_clause(this) }}
{%- endif %}
