{% materialization external_table, adapter='spark' -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = config.get('temp_table_name') %}
  {%- set options = config.get('options') -%}
  {%- set driver = config.get('driver') -%}

  {% set create_external_view %}
    CREATE TEMPORARY VIEW {{ tmp_relation_ext }}
    USING {{ driver }}
    {{ options_clause() }}
  {% endset %}

  {{ run_hooks(pre_hooks) }}

  {% do run_query(create_table_as(True, tmp_relation, sql)) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
