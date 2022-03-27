{% materialization external_table, adapter='spark' -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation_ext = make_temp_relation(this, '_ext') %}
  {% set tmp_relation = make_temp_relation(this) %}
  {%- set options = config.get('options') -%}
  {%- set driver = config.get('driver') -%}

  {% set create_external_view %}
    CREATE TEMPORARY VIEW {{ tmp_relation_ext }}
    USING {{ driver }}
    options (
      {%- if driver == 'com.audienceproject.spark.dynamodb.datasource' %}
        tableName "{{ target_relation }}"
      {% else %}
        dbtable "{{ target_relation }}"
      {% endif %}
      {%- if options is not none %}
        {%- for option in options -%}
        {{ option }} "{{ options[option] }}" {% if not loop.last %}, {% endif %}
        {%- endfor %}
      {%- endif %}
    )
  {% endset %}

  {{ run_hooks(pre_hooks) }}

  {% do run_query(create_external_view) %}
  {% do run_query(create_table_as(True, tmp_relation, sql)) %}
  {%- call statement('main') -%}
    INSERT INTO TABLE {{ tmp_relation_ext }}
    SELECT * FROM {{ tmp_relation }}
  {%- endcall -%}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
