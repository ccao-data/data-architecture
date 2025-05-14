/*
Macro that can selectively add:
- deactivated
- class 999
- non-condo class
- non-property tax exempt
rows to the open data views so that a ":deleted" flag associated with their
row_id can be sent to the open data portal.

There are multiple complications here:
- Feeder views can have different columns that define row_id. Currently, the
only case we are accomodating is res sf/mf data, which includes card in row_id
rather than just pin and year.
- The universe of parcels that might need to be purged from the open data assets
is different for different feeder views. The macro takes arguments to specify
how to construct the approriate universe of rows to purge.
*/
{% macro open_data_join_rows_to_delete(
    card=false, allow_999=false, condo=false, addn_table=none
) %}
    full outer join
        (
            select
                {% if addn_table == "dweldat" %}
                    pdat.parid || cast(addndat.card as varchar) || pdat.taxyr as row_id,
                {% elif addn_table == "htpar" %}
                    pdat.parid || pdat.taxyr || addndat.case_no as row_id,
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
                {% if addn_table == "htpar" %}
                    or addndat.caseno is not null or addndat.heartyp in ('A', 'C')
                {% endif %}
                {% if addn_table == "owndat" %} or addndat.ownnum is null {% endif %}
                {% if condo == true %} or pdat.class not in ('299', '399') {% endif %}
                {% if allow_999 == false %} or pdat.class = '999' {% endif %}

        ) as deleted_rows
        {% if addn_table == "dweldat" %}
            on feeder.pin || cast(feeder.card as varchar) || feeder.year
        {% elif addn_table == "htpar" %} on feeder.pin || feeder.year || feeder.case_no
        {% else %} on feeder.pin || feeder.year
        {% endif %} = deleted_rows.row_id
{% endmacro %}
