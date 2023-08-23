-- Override the default alias naming to automatically strip out the schema
-- prefix that we add to our models. For example, `iasworld.pardat` will
-- become `pardat` using this macro.
-- See: https://docs.getdbt.com/docs/build/custom-aliases#generate_alias_name
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name -%} {{ custom_alias_name | trim }}

    {%- else -%}

        {%- set node_name_parts = node.name.split(".") -%}

        {%- if node_name_parts | length < 2 -%}
            {#
                If the node name does not contain a leading prefix, it
                is not following our usual schema prefixing pattern, so assume
                that we don't want a custom alias for it
            #}
            {{ return(node.name) }}

        {%- endif -%}

        {{ return(node_name_parts[1:] | join("")) }}

    {%- endif -%}
{% endmacro %}
