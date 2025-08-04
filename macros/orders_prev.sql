{% macro check_table_prev(source_name_stored,table_name) %}
    select * from {{ source("{{source_name_stored}}", '{{table_name}}') }}
{% endmacro %}