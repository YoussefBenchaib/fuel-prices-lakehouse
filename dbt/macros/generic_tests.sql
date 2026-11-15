{# Fails for every group of rows that share the same values in `columns`. #}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}


{# Fails for every row whose value is outside [min, max]. Either bound can be omitted. #}
{% test between(model, column_name, min=none, max=none) %}
select {{ column_name }}
from {{ model }}
where false
    {% if min is not none %} or {{ column_name }} < {{ min }} {% endif %}
    {% if max is not none %} or {{ column_name }} > {{ max }} {% endif %}
{% endtest %}
