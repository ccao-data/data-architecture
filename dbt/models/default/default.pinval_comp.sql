{{
    config(
        materialized='table'
    )
}}

WITH runs_to_include AS (
    SELECT
        run_id,
        model_predictor_all_name
    FROM {{ source('model', 'metadata') }}
    WHERE run_id = '2025-02-11-charming-eric'
),

raw_comp AS (
    SELECT comp.*
    FROM model.comp AS comp
    INNER JOIN runs_to_include AS run
        ON comp.run_id = run.run_id
),

pivoted_comp AS (
    SELECT
        pin,
        card,
        1 AS comp_num,
        comp_pin_1 AS comp_pin,
        comp_score_1 AS comp_score,
        comp_document_num_1 AS comp_document_num,
        run_id
    FROM raw_comp

    UNION ALL
    SELECT
        pin,
        card,
        2 AS comp_num,
        comp_pin_2,
        comp_score_2,
        comp_document_num_2,
        run_id
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        card,
        3 AS comp_num,
        comp_pin_3,
        comp_score_3,
        comp_document_num_3,
        run_id
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        card,
        4 AS comp_num,
        comp_pin_4,
        comp_score_4,
        comp_document_num_4,
        run_id
    FROM raw_comp
    UNION ALL
    SELECT
        pin,
        card,
        5 AS comp_num,
        comp_pin_5,
        comp_score_5,
        comp_document_num_5,
        run_id
    FROM raw_comp
),

school_data AS (
    SELECT
        pin10 AS school_pin,
        year,
        school_elementary_district_name,
        school_secondary_district_name
    FROM location.school
    WHERE year > '2014'
),

comp_with_training_chars AS (
    SELECT
        pc.*,
        COALESCE(pc.pin = pc.comp_pin, FALSE) AS is_subject_pin_sale,
        CASE
            WHEN train.ind_pin_is_multicard = TRUE THEN 'Subject card'
            ELSE 'Subject property'
        END AS property_label,
        ARRAY_JOIN(
            TRANSFORM(
                SPLIT(LOWER(train.loc_property_address), ' '),
                x -> CONCAT(UPPER(SUBSTR(x, 1, 1)), SUBSTR(x, 2))
            ),
            ' '
        ) AS property_address,
        CAST(CAST(train.meta_sale_price / 1000 AS BIGINT) AS VARCHAR)
        || 'K' AS sale_price_short,
        ROUND(train.meta_sale_price / NULLIF(train.char_bldg_sf, 0))
            AS sale_price_per_sq_ft,
        train.*,
        school.school_elementary_district_name
            AS loc_school_elementary_district_name,
        school.school_secondary_district_name
            AS loc_school_secondary_district_name,
        meta.model_predictor_all_name
    FROM pivoted_comp AS pc
    LEFT JOIN model.pinval_test_training_data AS train
        ON pc.comp_pin = train.meta_pin
        AND pc.comp_document_num = train.meta_sale_document_num
    LEFT JOIN school_data AS school
        ON SUBSTRING(pc.comp_pin, 1, 10) = school.school_pin
        AND train.meta_year = school.year
    LEFT JOIN runs_to_include AS meta
        ON pc.run_id = meta.run_id
)

SELECT * FROM comp_with_training_chars
