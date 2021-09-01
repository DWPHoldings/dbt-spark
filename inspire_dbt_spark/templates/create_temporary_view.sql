CREATE TEMPORARY VIEW {{ alias }}
    USING {{ source_driver }}
    OPTIONS (
      {{ relation_type }} '{{ relation }}',
      {% for opt, val in options.items() %}
      {{ opt }} '{{ val }}'{%- if not loop.last %},{%- endif %}
      {% endfor %}
    )