{% macro decode_base64_safe(column_name, scale) %}
(
    CASE
        WHEN TRY_TO_NUMBER({{ column_name }}) IS NOT NULL
            THEN {{ column_name }}::NUMBER / POWER(10, {{ scale }})
        ELSE
            TRY_TO_NUMBER(
                TO_VARCHAR(
                    BASE64_DECODE_BINARY({{ column_name }})
                )
            ) / POWER(10, {{ scale }})
    END
)
{% endmacro %}