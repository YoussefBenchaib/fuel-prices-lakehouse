{#
  Average of a DECIMAL(6,3) price, rounded half up to three decimals with integer arithmetic.

  DuckDB's avg() returns a DOUBLE, so an average that falls exactly on a half (8.030 / 4 = 2.0075)
  can round down to 2.007 where Spark's decimal average gives 2.008. Working in milli-euros
  keeps the result exact and equal on both engines.
#}
{% macro exact_avg(column) %}
    cast(
        ((2 * sum(cast({{ column }} * 1000 as bigint)) + count(*)) // (2 * count(*))) / 1000.0
        as decimal(6, 3)
    )
{% endmacro %}
