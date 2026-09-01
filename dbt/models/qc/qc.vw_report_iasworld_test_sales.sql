{%- set tests = [
    {
        "name": "iasworld_sales_cur_in_accepted_values",
        "description": 'cur should be "Y" or "D"',
        "category": "incorrect_values",
        "condition": "cur IN ('Y', 'D')",
        "additional_select_columns": ["cur"]
    },
    {
        "name": "iasworld_sales_instrtyp_in_accepted_values",
        "description": "instrtyp should be '01', '02', '03', '04', '05', '06', or 'B'",
        "category": "incorrect_values",
        "condition": "instrtyp IN ('01', '02', '03', '04', '05', '06', 'B')",
        "additional_select_columns": ["instrtyp"]
    },
    {
        "name": "iasworld_sales_parid_not_null",
        "description": "parid should not be null",
        "category": "missing_values",
        "condition": "parid IS NOT NULL"
    },
    {
        "name": "iasworld_sales_parid_in_pardat_parid",
        "description": "parid should be in pardat",
        "category": "relationships",
        "condition": "pardat_parid IS NOT NULL"
    },
    {
        "name": "iasworld_sales_price_between_0_and_1b",
        "description": "price should be between 0 and 1,000,000,000",
        "category": "incorrect_values",
        "condition": "price IS NULL OR (price >= 0 AND price <= 1000000000)",
        "additional_select_columns": ["price"]
    },
    {
        "name": "iasworld_sales_saledt_lte_now",
        "description": "saledt should be before the current date",
        "category": "incorrect_values",
        "condition": "saledt IS NULL OR DATE_PARSE(SUBSTR(saledt, 1, 10), '%Y-%m-%d') <= current_date",
        "additional_select_columns": ["saledt"]
    },
    {
        "name": "iasworld_sales_unique_by_parid_instruno",
        "description": "sales should be unique by parid and instruno",
        "category": "duplicate_records",
        "condition": "num_duplicates = 1",
        "additional_select_columns": ["num_duplicates", "instruno"]
    }
] -%}

{%- set base_query %}
    SELECT
        -- Identifying columns
        sales.parid,
        SUBSTR(sales.saledt, 1, 4) AS taxyr,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.user1 AS township_code,
        pardat.class,
        sales.who,
        sales.wen,
        -- Columns to test
        sales.cur,
        sales.instrtyp,
        sales.price,
        sales.saledt,
        sales.instruno,
        pardat.parid AS pardat_parid,
        COUNT(*)
            OVER (PARTITION BY sales.parid, sales.instruno)
            AS num_duplicates
    FROM {{ source('iasworld', 'sales') }} AS sales
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
        ON sales.parid = legdat.parid
        AND SUBSTR(sales.saledt, 1, 4) = legdat.taxyr
        AND legdat.cur = 'Y'
        AND legdat.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
        ON sales.parid = pardat.parid
        AND SUBSTR(sales.saledt, 1, 4) = pardat.taxyr
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
    WHERE sales.cur = 'Y'
        AND sales.deactivat IS NULL
{% endset %}

{{ generate_iasworld_qc_test_view(base_query, tests) }}
