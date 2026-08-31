{%- set tests = [
    {
        "name": "iasworld_pardat_adrno_length_lte_5",
        "description": "adrno should be <= 5 characters long",
        "category": "column_length",
        "condition": "length(adrno) <= 5",
        "additional_select_columns": ["adrno"]
    },
    {
        "name": "iasworld_pardat_class_equals_luc",
        "description": "class should be the same as luc",
        "category": "class_mismatch_or_issue",
        "condition": "class = luc",
        "additional_select_columns": ["luc"]
    },
    {
        "name": "iasworld_pardat_class_in_ccao_class_dict",
        "description": "class_code should be valid",
        "category": "class_mismatch_or_issue",
        "condition": "class_dict_class IS NOT NULL OR class IN ('EX', 'RR') OR REGEXP_LIKE(class, '[0-9]{3}[A|B]')"
        "additional_select_columns": ["class"]
    },
    {
        "name": "iasworld_pardat_cur_in_accepted_values",
        "description": 'cur should be "Y" or "D"',
        "category": "incorrect_values",
        "condition": "cur IN ('Y', 'D')",
        "additional_select_columns": ["cur"]
    },
    {
        "name": "iasworld_pardat_nbhd_matches_legdat_township",
        "description": "nbhd code first 2 digits should match legdat.user1 (township code)",
        "category": "relationships",
        "condition": "SUBSTR(nbhd, 1, 2) = township_code",
        "additional_select_columns": ["nbhd"]
    },
    {
        "name": "iasworld_pardat_nbhd_matches_spatial_town_nbhd",
        "description": "nbhd code not valid",
        "category": "relationships",
        "condition": "town_nbhd IS NOT NULL OR nbhd LIKE '%999'",
        "additional_select_columns": ["nbhd"]
    },
    {
        "name": "iasworld_pardat_seq_all_sequential_exist",
        "description": "seq should be sequential",
        "category": "incorrect_values",
        "condition": "seq = prev_seq + 1",
        "additional_select_columns": ["seq", "prev_seq"]
    },
    {
        "name": "iasworld_pardat_unique_by_parid_taxyr",
        "description": "pardat should be unique by parid and taxyr",
        "category": "duplicate_records",
        "condition": "num_duplicates = 1",
        "additional_select_columns": ["num_duplicates"]
    }
] -%}

{%- set base_query %}
    SELECT
        -- Identifying columns
        pardat.parid,
        pardat.taxyr,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.user1 AS township_code,
        pardat.class,
        pardat.who,
        pardat.wen,
        -- Columns to test
        CAST(pardat.adrno AS VARCHAR) AS adrno,
        pardat.luc,
        pardat.cur,
        pardat.nbhd,
        nbhd.town_nbhd,
        class_dict.class_code AS class_dict_class,
        pardat.seq,
        LAG(pardat.seq)
            OVER (PARTITION BY pardat.parid, pardat.taxyr ORDER BY pardat.seq)
            AS prev_seq,
        COUNT(*)
            OVER (PARTITION BY pardat.parid, pardat.taxyr)
            AS num_duplicates
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
        AND legdat.cur = 'Y'
        AND legdat.deactivat IS NULL
    LEFT JOIN (
        SELECT DISTINCT town_nbhd
        FROM {{ source('spatial', 'neighborhood') }}
    ) AS nbhd
        ON pardat.nbhd = nbhd.town_nbhd
    LEFT JOIN {{ ref('ccao.class_dict') }} AS class_dict
        ON pardat.class = class_dict.class_code
    WHERE pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
{% endset %}

{{ generate_iasworld_qc_test_view(base_query, tests) }}
