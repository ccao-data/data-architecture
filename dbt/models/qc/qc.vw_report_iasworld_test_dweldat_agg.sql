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
        ) AS any_class_is_exempt_or_omitted
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
        AND dweldat.class NOT IN (
            '201', '213', '218', '219', '220', '221', '224', '225',
            '236', '240', '241', '290', '294', '297'
        )
        AND comdat.parid IS NULL
    GROUP BY dweldat.parid, dweldat.taxyr
{% endset %}

{{ generate_iasworld_qc_test_view(base_query, tests) }}
