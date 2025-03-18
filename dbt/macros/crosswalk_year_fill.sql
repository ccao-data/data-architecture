/*
Create a crosswalk for a set of features in a certain schema that a data
consumer can use in order to back- and forward-fill values for missing years.

We make two assumptions about input tables to this query:

1. The input data should already be forward-filled; that is to say, there
   should be a representation for every `year` starting from the earliest year
   of available data (i.e. the `data_year`) and going up through the current
   year
    * For example, if the first `data_year` is 2017, we expect there to be
      records for all years from 2017 to present
2. The input data should _not_ be backfilled; that is to say, the earliest
   `year` of data in the input table should correspond to the earliest
   `data_year`
    * For example, if the first `data_year` is 2017, we expect there to be
      no records with `year` values prior to 2017
    * We make an exemption for input tables that contain multiple variables
      (like `location.political`), in which case we expect the earliest `year`
      to correspond to the earliest `data_year` among _all_ of the variables.
      For example, if the earliest `chicago_ward_data_year` is 2017, but there
      exists a row with an `evanston_ward_data_year` of 2016, then we assume
      that the earliest `year` in the view will be 2016, and any rows with this
      `year` will have a null value for `chicago_ward_data_year`

Taken together, these two assumptions mean that this query _only_ performs
backfilling for all input data, and leaves forward-filling to the queries
that produce the input tables. This might seem like a strange choice, but it
stems from the fact that we need to forward-fill different data sources
slightly differently, while we perform backfilling in the same way across
all data sources.

The output of this query contains three different variations on "year" columns:

1. `year` is the PIN year; there should be one `year` for every year that
   exists in `spatial.parcel`
2. `{feature_name}_fill_year` is the year of data for a PIN in the input data
   that should be used to fill records with a given `year`
3. `{feature_name}_data_year` is the original year in the raw data that
   produced the value that exists in the fill year

Take this hypothetical row as an example:

| year | census_fill_year | census_data_year |
| ---- | ---------------- | ---------------- |
| 2019 | 2015             | 2017             |

This row means that for a PIN in year 2010, a data consumer can get a filled
value for a `location.census` feature by joining to the representation of that
PIN in the `location.census` table with year 2015, and that this particular row
of data in `location.census` uses a value that was recorded in the raw data in
2017.

So if we want to use the crosswalk to fill data in this case, we can write a
query like this:

    SELECT *
    FROM spatial.parcel AS pin
    INNER JOIN location.crosswalk_year_fill AS cyf
        ON pin.year = cyf.year
    LEFT JOIN location.census AS census
        ON pin.pin10 = census.pin10
        AND cyf.census_fill_year = census.year

Arguments:

    * schema (str): The schema (database) containing the tables described by
        the `fields` argument
    * fields (dict): Dict where keys are tablenames in the schema described by
        the `schema` argument and values are a list of names of `data_year`
        fields contained in each table that we want to use for filling
*/
{% macro crosswalk_year_fill(schema, fields) %}
    /*
    For each year of data we have in `spatial.parcel`, get the corresponding
    fill year and data year for features in our input tables. We expect that
    input tables will already be filled up to their earliest date of available
    data.

    As an example, assume we have Census data for the year 2020, but no other
    years aside from that year; also, assume that we have parcel data for years
    2018-2022. In this hypothetical case, the output of the `unfilled` subquery
    will look like this if we filter for only Census-related fields:

    | year | census_fill_year | census_data_year |
    | ---- | ---------------- | ---------------- |
    | 2018 |                  |                  |
    | 2019 |                  |                  |
    | 2020 | 2020             | 2020             |
    | 2021 | 2021             | 2020             |
    | 2022 | 2022             | 2020             |
    */
    with
        unfilled as (
            select
                pin.year,
                {% for tablename, fieldnames in fields.items() %}
                    {#
                        Preserve the outer loop indicator so that we can
                        combine it with the inner loop indicator to check
                        whether the last line of the block should end in a
                        trailing comma
                    #}
                    {%- set outer_loop_last = loop.last %}
                    {% for fieldname in fieldnames %}
                        {%- set inner_loop_last = loop.last %}
                        {#
                            Use MAX to remove nulls in cases of years where
                            some PINs have no match in the source data, since
                            the DISTINCT subqueries in the list of joins below
                            will still return those nulls
                        #}
                        max(
                            {{ tablename }}.{{ fieldname }}_data_year
                        ) as {{ fieldname }}_data_year,
                        {#
                            Only pull the year as the fill year for years where
                            we also have a data year, since otherwise we risk
                            pulling fill years for years that don't actually
                            have data. This is a particular risk for tables
                            that contain multiple features (e.g.
                            `location.political`) because it's possible for
                            example field A to have data for a given year while
                            example field B does not, in which case a naive
                            `MAX(year)` call would pull a fill year for the
                            field A even though data only exists for that year
                            in field B
                        #}
                        max(
                            case
                                when
                                    {{ tablename }}.{{ fieldname }}_data_year
                                    is not null
                                then {{ tablename }}.year
                                else null
                            end
                        ) as {{ fieldname }}_fill_year
                        {%- if not outer_loop_last or not inner_loop_last -%}
                            ,
                        {%- endif -%}
                    {% endfor %}
                {% endfor %}
            from (select distinct year from {{ source("spatial", "parcel") }}) as pin
            {% for tablename, fieldnames in fields.items() %}
                left join
                    (
                        select distinct
                            year,
                            {% for fieldname in fieldnames %}
                                {{ fieldname }}_data_year
                                {%- if not loop.last -%}, {%- endif -%}
                            {% endfor %}
                        from {{ ref(schema + "." + tablename) }}
                    ) as {{ tablename }}
                    on pin.year = {{ tablename }}.year
            {% endfor %}
            group by pin.year
        )

    /*
    Now that we have all parcel years and the earliest year of data in our
    location data sources, we can backfill years prior to the first year of
    available data by using that first year.

    Revisiting our example from the comment above the `unfilled` subquery,
    here's the output we expect for the final query for fields that use Census
    data years (note that the missing years 2018-2019 are now backfilled):

    | year | census_fill_year | census_data_year |
    | ---- | ---------------- | ---------------- |
    | 2018 | 2020             | 2020             |
    | 2019 | 2020             | 2020             |
    | 2020 | 2020             | 2020             |
    | 2021 | 2021             | 2020             |
    | 2022 | 2022             | 2020             |
    */
    select
        unfilled.year,
        {% for tablename, fieldnames in fields.items() %}
            {%- set outer_loop_last = loop.last %}
            {% for fieldname in fieldnames %}
                {%- set inner_loop_last = loop.last %}
                coalesce(
                    {{ fieldname }}_fill_year,
                    {#-
                        Use a window function to fill nulls with the first year
                        of available data for this data source
                    -#}
                    last_value({{ fieldname }}_fill_year)
                    ignore nulls over (order by unfilled.year desc)
                ) as {{ fieldname }}_fill_year,
                coalesce(
                    {{ fieldname }}_data_year,
                    last_value({{ fieldname }}_data_year)
                    ignore nulls over (order by unfilled.year desc)
                ) as {{ fieldname }}_data_year
                {%- if not outer_loop_last or not inner_loop_last -%}, {%- endif -%}
            {% endfor %}
        {% endfor %}
    from unfilled
    order by unfilled.year
{% endmacro %}
