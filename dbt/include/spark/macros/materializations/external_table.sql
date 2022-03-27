{% materialization external_table, adapter='spark' -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}

  {{ run_hooks(pre_hooks) }}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(True, target_relation, sql) }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
