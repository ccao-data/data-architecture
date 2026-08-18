WITH legdat_townships AS (
    SELECT
        parid,
        taxyr,
        user1 AS township_code
    FROM {{ source('iasworld', 'legdat') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
),

distinct_town_nbhd AS (
    SELECT DISTINCT town_nbhd
    FROM {{ source('spatial', 'neighborhood') }}
),

pardat_seq AS (
    SELECT
        parid,
        taxyr,
        class,
        seq,
        LAG(seq) OVER (PARTITION BY parid, taxyr ORDER BY seq) AS prev_seq,
        who,
        wen
    FROM {{ source('iasworld', 'pardat') }}
    WHERE CAST(taxyr AS INT) BETWEEN
        {{ var('data_test_iasworld_year_start') }}
        AND {{ var('data_test_iasworld_year_end') }}
        AND cur = 'Y'
        AND deactivat IS NULL
),

iasworld_pardat_adrno_length_lte_5 AS (
    SELECT
        'iasworld_pardat_adrno_length_lte_5' AS test_name,
        'adrno should be <= 5 characters long' AS test_description,
        'column_length' AS test_category,
        pardat.taxyr,
        pardat.parid,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.township_code,
        pardat.class,
        pardat.who,
        pardat.wen,
        MAP(
            ARRAY['adrno'],
            ARRAY[CAST(pardat.adrno AS VARCHAR)]
        ) AS additional_fields
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN legdat_townships AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
    WHERE NOT (LENGTH(CAST(pardat.adrno AS VARCHAR)) <= 5)
        AND CAST(pardat.taxyr AS INT) BETWEEN
        {{ var('data_test_iasworld_year_start') }}
        AND {{ var('data_test_iasworld_year_end') }}
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
),

iasworld_pardat_class_equals_luc AS (
    SELECT
        'iasworld_pardat_class_equals_luc' AS test_name,
        'class should be the same as luc' AS test_description,
        'class_mismatch_or_issue' AS test_category,
        pardat.taxyr,
        pardat.parid,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.township_code,
        pardat.class,
        pardat.who,
        pardat.wen,
        MAP(
            ARRAY['luc'],
            ARRAY[pardat.luc]
        ) AS additional_fields
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN legdat_townships AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
    WHERE NOT (pardat.class = pardat.luc)
        AND CAST(pardat.taxyr AS INT) BETWEEN
        {{ var('data_test_iasworld_year_start') }}
        AND {{ var('data_test_iasworld_year_end') }}
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
),

iasworld_pardat_cur_in_accepted_values AS (
    SELECT
        'iasworld_pardat_cur_in_accepted_values' AS test_name,
        'cur should be ''Y'' or ''D''' AS test_description,
        CAST(NULL AS VARCHAR) AS test_category,
        pardat.taxyr,
        pardat.parid,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.township_code,
        pardat.class,
        pardat.who,
        pardat.wen,
        MAP(
            ARRAY['cur'],
            ARRAY[pardat.cur]
        ) AS additional_fields
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN legdat_townships AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
    WHERE pardat.cur NOT IN ('Y', 'D')
        AND CAST(pardat.taxyr AS INT) BETWEEN
        {{ var('data_test_iasworld_year_start') }}
        AND {{ var('data_test_iasworld_year_end') }}
),

iasworld_pardat_nbhd_matches_legdat_township AS (
    SELECT
        'iasworld_pardat_nbhd_matches_legdat_township' AS test_name,
        'nbhd code first 2 digits should match legdat.user1 (township code)'
            AS test_description,
        'relationships' AS test_category,
        pardat.taxyr,
        pardat.parid,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.township_code,
        pardat.class,
        pardat.who,
        pardat.wen,
        MAP(
            ARRAY['nbhd'],
            ARRAY[pardat.nbhd]
        ) AS additional_fields
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    INNER JOIN legdat_townships AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
        AND SUBSTR(pardat.nbhd, 1, 2) != legdat.township_code
    WHERE CAST(pardat.taxyr AS INT) BETWEEN
        {{ var('data_test_iasworld_year_start') }}
        AND {{ var('data_test_iasworld_year_end') }}
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
        AND pardat.nbhd NOT LIKE '%999'
),

iasworld_pardat_nbhd_matches_spatial_town_nbhd AS (
    SELECT
        'iasworld_pardat_nbhd_matches_spatial_town_nbhd' AS test_name,
        'nbhd code not valid' AS test_description,
        'relationships' AS test_category,
        pardat.taxyr,
        pardat.parid,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.township_code,
        pardat.class,
        pardat.who,
        pardat.wen,
        MAP(
            ARRAY['nbhd'],
            ARRAY[pardat.nbhd]
        ) AS additional_fields
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN legdat_townships AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
    LEFT JOIN distinct_town_nbhd
        ON pardat.nbhd = distinct_town_nbhd.town_nbhd
    WHERE distinct_town_nbhd.town_nbhd IS NULL
        AND CAST(pardat.taxyr AS INT) BETWEEN
        {{ var('data_test_iasworld_year_start') }}
        AND {{ var('data_test_iasworld_year_end') }}
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
        AND pardat.nbhd NOT LIKE '%999'
),

iasworld_pardat_seq_all_sequential_exist AS (
    SELECT
        'iasworld_pardat_seq_all_sequential_exist' AS test_name,
        'seq should be sequential' AS test_description,
        CAST(NULL AS VARCHAR) AS test_category,
        pardat_seq.taxyr,
        pardat_seq.parid,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.township_code,
        pardat_seq.class,
        pardat_seq.who,
        pardat_seq.wen,
        MAP(
            ARRAY['seq', 'prev_seq'],
            ARRAY[
                CAST(pardat_seq.seq AS VARCHAR),
                CAST(pardat_seq.prev_seq AS VARCHAR)
            ]
        ) AS additional_fields
    FROM pardat_seq
    LEFT JOIN legdat_townships AS legdat
        ON pardat_seq.parid = legdat.parid
        AND pardat_seq.taxyr = legdat.taxyr
    WHERE NOT (pardat_seq.seq = pardat_seq.prev_seq + 1)
),

iasworld_pardat_unique_by_parid_taxyr AS (
    SELECT
        'iasworld_pardat_unique_by_parid_taxyr' AS test_name,
        'pardat should be unique by parid and taxyr' AS test_description,
        CAST(NULL AS VARCHAR) AS test_category,
        pardat.taxyr,
        pardat.parid,
        CAST(NULL AS INTEGER) AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.township_code,
        pardat.class,
        pardat.who,
        pardat.wen,
        MAP(
            ARRAY['num_duplicates'],
            ARRAY[CAST(dupe_counts.num_dupes AS VARCHAR)]
        ) AS additional_fields
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN legdat_townships AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
    INNER JOIN (
        SELECT
            parid,
            taxyr,
            COUNT(*) AS num_dupes
        FROM {{ source('iasworld', 'pardat') }}
        WHERE CAST(taxyr AS INT) BETWEEN
            {{ var('data_test_iasworld_year_start') }}
            AND {{ var('data_test_iasworld_year_end') }}
            AND cur = 'Y'
            AND deactivat IS NULL
        GROUP BY parid, taxyr
        HAVING COUNT(*) > 1
    ) AS dupe_counts
        ON pardat.parid = dupe_counts.parid
        AND pardat.taxyr = dupe_counts.taxyr
    WHERE CAST(pardat.taxyr AS INT) BETWEEN
        {{ var('data_test_iasworld_year_start') }}
        AND {{ var('data_test_iasworld_year_end') }}
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
)

SELECT * FROM iasworld_pardat_adrno_length_lte_5
UNION ALL
SELECT * FROM iasworld_pardat_class_equals_luc
UNION ALL
SELECT * FROM iasworld_pardat_cur_in_accepted_values
UNION ALL
SELECT * FROM iasworld_pardat_nbhd_matches_legdat_township
UNION ALL
SELECT * FROM iasworld_pardat_nbhd_matches_spatial_town_nbhd
UNION ALL
SELECT * FROM iasworld_pardat_seq_all_sequential_exist
UNION ALL
SELECT * FROM iasworld_pardat_unique_by_parid_taxyr
