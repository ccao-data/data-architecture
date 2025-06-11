WITH runs_to_include AS (
    SELECT
        run_id,
        model_predictor_all_name
    FROM {{ source('model', 'metadata') }}
    WHERE run_id = 'comps'
),

raw_comp AS (
    SELECT comp.*
    FROM {{ source('model', 'comp') }} AS comp
    INNER JOIN runs_to_include AS run
        ON comp.run_id = run.run_id
),

pivoted_comp AS (
    {% for i in range(1, 6) %}
        SELECT
            pin,
            card,
            {{ i }} AS comp_num,
            comp_pin_{{ i }} AS comp_pin,
            comp_score_{{ i }} AS comp_score,
            comp_document_num_{{ i }} AS comp_document_num,
            run_id AS comps_run_id
        FROM raw_comp
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
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

training_data_rename_run_id AS (
    SELECT
        *,
        run_id AS model_train_run_id
    FROM {{ ref('model.training_data') }}
)

SELECT
    pc.*,
    COALESCE(pc.pin = pc.comp_pin, FALSE) AS is_subject_pin_sale,
    CASE
        WHEN train.ind_pin_is_multicard = TRUE THEN 'Subject card'
        ELSE 'Subject property'
    END AS property_label,
    train.loc_property_address AS property_address,
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
LEFT JOIN training_data_rename_run_id AS train
    ON pc.comp_pin = train.meta_pin
    AND pc.comp_document_num = train.meta_sale_document_num
LEFT JOIN school_data AS school
    ON SUBSTRING(pc.comp_pin, 1, 10) = school.school_pin
    AND train.meta_year = school.year
LEFT JOIN runs_to_include AS meta
    ON pc.run_id = meta.run_id
