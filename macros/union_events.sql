{% macro union_events(table_prefix, event_suffixes) %}

    {% for suffix in event_suffixes %}
        
        select * from {{ ref(table_prefix ~ '_' ~ suffix) }}
        
        {% if not loop.last %}
            union all
        {% endif %}

    {% endfor %}

{% endmacro %}