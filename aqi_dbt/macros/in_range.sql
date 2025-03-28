{% macro test_in_range(model, column_name, min, max) %}
  with failed_rows as (
    select
      {{ column_name }}
    from {{ model }}
    where {{ column_name }} < {{ min }} or {{ column_name }} > {{ max }}
  )

  select count(*) = 0 as pass
  from failed_rows
{% endmacro %}