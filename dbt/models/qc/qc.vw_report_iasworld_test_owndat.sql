{%- set tests = [
    {
        "name": "iasworld_owndat_cur_in_accepted_values",
        "description": 'cur should be "Y" or "D"',
        "category": "incorrect_values",
        "condition": "cur IN ('Y', 'D')",
        "where": "",
        "additional_select_columns": ["cur"]
    },
    {
        "name": "iasworld_owndat_parid_not_null",
        "description": "parid should not be null",
        "category": "missing_values",
        "condition": "parid IS NOT NULL"
    },
    {
        "name": "iasworld_owndat_seq_all_sequential_exist",
        "description": "seq should be sequential",
        "category": "incorrect_values",
        "condition": "seq = prev_seq + 1",
        "additional_select_columns": ["seq", "prev_seq"]
    },
    {
        "name": "iasworld_owndat_unique_by_parid_taxyr",
        "description": "owndat should be unique by parid and taxyr",
        "category": "duplicate_records",
        "condition": "num_duplicates = 1",
        "additional_select_columns": ["num_duplicates"]
    }
] -%}

{%- set default_where = "cur = 'Y' AND deactivat IS NULL" -%}

{%- set base_query %}
    SELECT
        -- Identifying columns
        owndat.parid,
        owndat.taxyr,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.user1 AS township_code,
        CAST(NULL AS VARCHAR) AS class,
        owndat.who,
        owndat.wen,
        -- Columns to test
        owndat.cur,
        owndat.deactivat,
        owndat.seq,
        owndat.prev_seq,
        owndat.num_duplicates
    FROM (
        -- Compute window functions over the source table alone, partitioned
        -- by default_where so that records that don't pass the default
        -- filters can't affect the results for records that do
        SELECT
            *,
            LAG(seq)
                OVER (
                    PARTITION BY parid, taxyr, ({{ default_where }})
                    ORDER BY seq
                )
                AS prev_seq,
            COUNT(*)
                OVER (PARTITION BY parid, taxyr, ({{ default_where }}))
                AS num_duplicates
        FROM {{ source('iasworld', 'owndat') }}
    ) AS owndat
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
        ON owndat.parid = legdat.parid
        AND owndat.taxyr = legdat.taxyr
        AND legdat.cur = 'Y'
        AND legdat.deactivat IS NULL
    INNER JOIN {{ source('iasworld', 'pardat') }} AS pardat
        ON owndat.parid = pardat.parid
        AND owndat.taxyr = pardat.taxyr
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
{% endset %}

{{ generate_iasworld_qc_test_view(
    base_query, tests, default_where=default_where
) }}
