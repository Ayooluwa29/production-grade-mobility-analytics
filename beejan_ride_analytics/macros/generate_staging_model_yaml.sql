{% macro generate_staging_model_yaml() %}
    {% set models_to_generate = codegen.get_models(directory='staging') %}
    {{ codegen.generate_model_yaml(
        model_names = models_to_generate
    ) }}
{% endmacro %}