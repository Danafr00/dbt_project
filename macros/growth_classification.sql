{# This macro returns the description of the growth #}

{% macro growth_classification(persen_growth) -%}

    CASE 
        WHEN {{ persen_growth }} < -5 
            THEN 'buruk sekali'
        WHEN {{ persen_growth }} < -1 AND {{ persen_growth }} > -5 
            THEN 'buruk'
        WHEN {{ persen_growth }} < 1 AND {{ persen_growth }} > -1 
            THEN 'stagnan'
        WHEN {{ persen_growth }} > 1 AND {{ persen_growth }} < 5 
            THEN 'baik'
        ELSE 'baik sekali'
    END

{%- endmacro %}
