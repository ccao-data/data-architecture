WITH base AS (
    SELECT
        flag.meta_sale_document_num,
        flag.run_id,
        (
            group_mean.group_size >= param.min_group_thresh
        ) AS meets_group_threshold,
        flag."group",
        group_mean.group_size,
        -- flag.sv_price_deviation,
        -- flag.sv_price_per_sqft_deviation,
        flag.meta_sale_price_original,
        flag.sv_is_outlier,
        flag.ptax_flag_original,
        flag.sv_is_ptax_outlier,
        flag.sv_outlier_reason1,
        flag.sv_outlier_reason2,
        flag.sv_outlier_reason3,
        param.stat_groups,
        param.housing_market_class_codes,
        pin_sale.pin,
        pin_sale.year,
        p_uni.triad_code,
        p_uni.class
    FROM {{ source('sale', 'flag') }} AS flag
    LEFT JOIN {{ source('sale', 'group_mean') }} AS group_mean
        ON flag.run_id = group_mean.run_id
       AND flag."group" = group_mean."group"
    LEFT JOIN {{ source('sale', 'parameter') }} AS param
        ON flag.run_id = param.run_id
    LEFT JOIN {{ ref('default.vw_pin_sale') }} AS pin_sale
        ON pin_sale.doc_no = flag.meta_sale_document_num
    LEFT JOIN {{ ref('default.vw_pin_universe') }} AS p_uni
        ON p_uni.pin = pin_sale.pin
       AND p_uni.year = pin_sale.year
),

-- Normalize JSON: try parsing as-is; if NULL, retry with single quotes swapped
normalized_json AS (
    SELECT
        base.*,
        COALESCE(
            TRY(JSON_PARSE(base.stat_groups)),
            TRY(JSON_PARSE(REPLACE(base.stat_groups, '''', '"')))
        ) AS stat_groups_json,
        COALESCE(
            TRY(JSON_PARSE(base.housing_market_class_codes)),
            TRY(JSON_PARSE(REPLACE(base.housing_market_class_codes, '''', '"')))
        ) AS housing_json
    FROM base
),

-- Pull triad number (digits only) and carry forward JSON
triad_only AS (
    SELECT
        norm_json.*,
        NULLIF(
            REGEXP_REPLACE(
                LOWER(
                    TRIM(CAST(norm_json.triad_code AS VARCHAR))
                ),
                '[^0-9]+',
                ''
            ),
            ''
        ) AS tri_num
    FROM normalized_json AS norm_json
),

-- Constrain class matching to submarkets that actually exist under tri{n}
effective_key AS (
    SELECT
        triad_only.*,

        -- Keys present under tri{n} that have a .columns array
        CASE
            WHEN triad_only.tri_num IS NOT NULL
            THEN MAP_KEYS(
                MAP_FILTER(
                    CAST(
                        JSON_EXTRACT(
                            triad_only.stat_groups_json,
                            FORMAT('$.tri%s', triad_only.tri_num)
                        ) AS MAP (VARCHAR, JSON)
                    ),
                    (k, v) ->
                        JSON_ARRAY_LENGTH(
                            JSON_EXTRACT(v, '$.columns')
                        ) IS NOT NULL
                )
            )
            ELSE CAST(ARRAY[] AS ARRAY (VARCHAR))
        END AS keys_present,

        -- Keys (submarkets) whose class list contains this row's class
        CASE
            WHEN triad_only.housing_json IS NOT NULL
            THEN MAP_KEYS(
                MAP_FILTER(
                    CAST(
                        triad_only.housing_json AS MAP (VARCHAR, JSON)
                    ),
                    (k, v) ->
                        CONTAINS(
                            CAST(v AS ARRAY (VARCHAR)),
                            CAST(triad_only.class AS VARCHAR)
                        )
                )
            )
            ELSE CAST(ARRAY[] AS ARRAY (VARCHAR))
        END AS keys_for_class
    FROM triad_only
),

-- Choose effective housing key:
-- 1) intersection(keys_present, keys_for_class) if any
-- 2) otherwise any keys_present (fallback)
choose_key AS (
    SELECT
        eff_key.*,

        -- Intersection via FILTER to avoid ARRAY_INTERSECT dependency
        FILTER(
            eff_key.keys_present,
            k -> CONTAINS(eff_key.keys_for_class, k)
        ) AS present_and_matching,

        CASE
            WHEN CARDINALITY(
                     FILTER(
                         eff_key.keys_present,
                         k -> CONTAINS(eff_key.keys_for_class, k)
                     )
                 ) > 0
            THEN FILTER(
                     eff_key.keys_present,
                     k -> CONTAINS(eff_key.keys_for_class, k)
                 )[1]
            WHEN CARDINALITY(eff_key.keys_present) > 0
            THEN eff_key.keys_present[1]
            ELSE NULL
        END AS effective_housing_key
    FROM effective_key AS eff_key
),

-- Pull columns array for tri{n}.{effective_housing_key}.columns
cols_json AS (
    SELECT
        choose_key.*,
        CASE
            WHEN choose_key.tri_num IS NOT NULL
             AND choose_key.effective_housing_key IS NOT NULL
            THEN JSON_EXTRACT(
                     choose_key.stat_groups_json,
                     FORMAT(
                         '$.tri%s.%s.columns',
                         choose_key.tri_num,
                         choose_key.effective_housing_key
                     )
                 )
            ELSE NULL
        END AS columns_json
    FROM choose_key
)

SELECT
    meta_sale_document_num,
    run_id,
    meets_group_threshold,
    "group",
    group_size,
    -- sv_price_deviation,
    -- sv_price_per_sqft_deviation,
    meta_sale_price_original,
    sv_is_outlier,
    ptax_flag_original,
    sv_is_ptax_outlier,
    sv_outlier_reason1,
    sv_outlier_reason2,
    sv_outlier_reason3,
    pin,
    year,
    triad_code,
    class,
    -- Convert the columns array into ARRAY<VARCHAR> of column names,
    -- handling scalar strings and {"column": "..."} objects
    CASE
        WHEN columns_json IS NULL
          OR JSON_ARRAY_LENGTH(columns_json) = 0
        THEN CAST(ARRAY[] AS ARRAY (VARCHAR))
        ELSE TRANSFORM(
                 SEQUENCE(
                     0,
                     JSON_ARRAY_LENGTH(columns_json) - 1
                 ),
                 i -> COALESCE(
                     JSON_EXTRACT_SCALAR(
                         columns_json,
                         FORMAT('$[%s].column', i)
                     ),
                     JSON_EXTRACT_SCALAR(
                         columns_json,
                         FORMAT('$[%s]', i)
                     )
                 )
             )
    END AS groups_used
FROM cols_json;
