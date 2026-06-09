{% macro decode_base64(column_name, scale) %}
    UNPACK({{ column_name }}, {{ scale }})
{% endmacro %}