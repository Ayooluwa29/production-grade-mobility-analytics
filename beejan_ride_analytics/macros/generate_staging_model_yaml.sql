{% macro generate_staging_model_yaml() %}
    {% set models_to_generate = codegen.get_models(directory='staging') %}

    {% set model_names = [] %}
    {% for model in models_to_generate %}
        {% do model_names.append(model.name) %}
    {% endfor %}

    {{ codegen.generate_model_yaml(
        model_names = model_names
    ) }}
{% endmacro %}