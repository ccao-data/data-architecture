/*
Macro that can selectively add:
- deactivated
- class 999
- non-condo class
rows to the open data views, as well as rows from tables other than pardat so
that a ":deleted" flag associated with their row_id can be sent to the open data
portal.

There are multiple complications here:
- Feeder views can have different columns that define row_id.
- The universe of parcels that might need to be purged from the open data assets
is different for different feeder views. The macro takes arguments to specify
how to construct the approriate universe of rows to purge.
*/
{% macro open_data_join_rows_to_delete(allow_999=false, condo=false, addn_table=none) %}
    full outer join
        (
            select
                {% if addn_table == "dweldat" %}
                    pdat.parid || cast(addndat.card as varchar) || pdat.taxyr as row_id,
                {% else %} pdat.parid || pdat.taxyr as row_id,
                {% endif %}
                pdat.taxyr as year,
                true as ":deleted"
            from {{ source("iasworld", "pardat") }} as pdat
            {% if addn_table is not none %}
                {% if addn_table == "dweldat" %} inner join
                {% else %} left join
                {% endif %}
                    {{ source("iasworld", addn_table) }} as addndat
                    on pdat.parid = addndat.parid
                    and pdat.taxyr = addndat.taxyr
            {% endif %}
            where
                pdat.deactivat is not null
                {% if addn_table == "dweldat" %}
                    or addndat.deactivat is not null
                {% endif %}
                {% if addn_table == "owndat" %} or addndat.ownnum is null {% endif %}
                {% if condo == true %} or pdat.class not in ('299', '399') {% endif %}
                {% if allow_999 == false %} or pdat.class = '999' {% endif %}
        ) as deleted_rows
        {% if addn_table == "dweldat" %}
            on feeder.pin || cast(feeder.card as varchar) || feeder.year
        {% else %} on feeder.pin || feeder.year
        {% endif %} = deleted_rows.row_id
{% endmacro %}
