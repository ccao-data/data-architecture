{%- set tests = [
    {
        "name": "iasworld_dweldat_class_matches_pardat_class",
        "description": "at least one class should match pardat class",
        "category": "class_mismatch_or_issue",
        "condition": "any_class_matches_pardat_class",
        "additional_select_columns": ["pardat_class", "classes"]
    },
    {
        "name": "iasworld_dweldat_exempt_classes_match_pardat_class",
        "description": (
            "at least one class should be exempt or omitted if pardat is "
            "exempt"
        ),
        "category": "class_mismatch_or_issue",
        "condition": "pardat_class != 'EX' OR any_class_is_exempt_or_omitted",
        "additional_select_columns": ["pardat_class", "classes"]
    },
    {
        "name": "iasworld_dweldat_class_in_ccao_class_dict",
        "description": "class code must be valid for every card",
        "category": "class_mismatch_or_issue",
        "condition": "NOT any_class_invalid",
        "additional_select_columns": ["classes"]
    }
] -%}

{%- set base_query %}
    SELECT
        -- Identifying columns
        dweldat.parid,
        dweldat.taxyr,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        MAX(legdat.user1) AS township_code,
        CAST(NULL AS VARCHAR) AS class,
        MAX(dweldat.who) AS who,
        MAX(dweldat.wen) AS wen,
        -- Computed columns for tests
        MAX(pardat.class) AS pardat_class,
        ARRAY_JOIN(ARRAY_AGG(dweldat.class), ', ') AS classes,
        BOOL_OR(dweldat.class = pardat.class) AS any_class_matches_pardat_class,
        BOOL_OR(
            dweldat.class LIKE 'OA%' OR dweldat.class = 'EX'
        ) AS any_class_is_exempt_or_omitted,
        BOOL_OR(
            dweldat.class NOT IN ('EX', 'RR') AND class_dict.class_code IS NULL
        ) AS any_class_invalid
    FROM {{ source('iasworld', 'dweldat') }} AS dweldat
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
        ON dweldat.parid = legdat.parid
        AND dweldat.taxyr = legdat.taxyr
        AND legdat.cur = 'Y'
        AND legdat.deactivat IS NULL
    INNER JOIN {{ source('iasworld', 'pardat') }} AS pardat
        ON dweldat.parid = pardat.parid
        AND dweldat.taxyr = pardat.taxyr
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
    LEFT JOIN {{ ref('ccao.class_dict') }} AS class_dict
        ON dweldat.class = class_dict.class_code
    -- Excludes mixed-use/commercial parcels, which are not subject to these
    -- residential class checks
    LEFT JOIN (
        SELECT DISTINCT parid, taxyr
        FROM {{ source('iasworld', 'comdat') }}
        WHERE cur = 'Y'
            AND deactivat IS NULL
    ) AS comdat
        ON dweldat.parid = comdat.parid
        AND dweldat.taxyr = comdat.taxyr
    WHERE dweldat.cur = 'Y'
        AND dweldat.deactivat IS NULL
        AND comdat.parid IS NULL
    GROUP BY dweldat.parid, dweldat.taxyr
{% endset %}

{{ generate_iasworld_qc_test_view(base_query, tests) }}
