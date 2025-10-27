WITH base AS (
    SELECT
        flag.meta_sale_document_num,
        flag.run_id,
        (group_mean.group_size >= param.min_group_thresh) AS meets_group_threshold,
        flag."group",
        group_mean.group_size,
        --flag.sv_price_deviation,
        --flag.sv_price_per_sqft_deviation,
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
        pin_sale."year",
        p_uni.triad_code,
        p_uni.class
    FROM sale.flag AS flag
    LEFT JOIN sale.group_mean AS group_mean
        ON flag.run_id = group_mean.run_id
       AND flag."group" = group_mean."group"
    LEFT JOIN sale.parameter AS param
        ON flag.run_id = param.run_id
    LEFT JOIN default.vw_pin_sale AS pin_sale
        ON pin_sale.doc_no = flag.meta_sale_document_num
    LEFT JOIN default.vw_pin_universe AS p_uni
        ON p_uni.pin = pin_sale.pin
       AND p_uni."year" = pin_sale."year"
),

-- TODO: do we need this?
-- Normalize JSON: try parsing as-is; if NULL, retry after swapping single quotes to double quotes
normalized_json AS (
    SELECT
        base.*,
        coalesce(
            try(json_parse(base.stat_groups)),
            try(json_parse(replace(base.stat_groups, '''', '"')))
        ) AS stat_groups_json,
        coalesce(
            try(json_parse(base.housing_market_class_codes)),
            try(json_parse(replace(base.housing_market_class_codes, '''', '"')))
        ) AS housing_json
    FROM base base
),

-- Determine triad number & desired housing key from parsed JSON
triad_and_key AS (
    SELECT
        n.*,

        -- keep only digits from triad_code (e.g., "tri3", "3", etc.)
        NULLIF(regexp_replace(lower(trim(CAST(n.triad_code AS VARCHAR))), '[^0-9]+', ''), '') AS tri_num,

        -- Map class -> desired housing_key using housing_json
        CASE
            WHEN coalesce(
                     contains(
                         CAST(json_extract(n.housing_json, '$.res_single_family') AS ARRAY(VARCHAR)),
                         CAST(n.class AS VARCHAR)
                     ), false
                 ) THEN 'res_single_family'
            WHEN coalesce(
                     contains(
                         CAST(json_extract(n.housing_json, '$.res_multi_family') AS ARRAY(VARCHAR)),
                         CAST(n.class AS VARCHAR)
                     ), false
                 ) THEN 'res_multi_family'
            WHEN coalesce(
                     contains(
                         CAST(json_extract(n.housing_json, '$.condos') AS ARRAY(VARCHAR)),
                         CAST(n.class AS VARCHAR)
                     ), false
                 ) THEN 'condos'
            WHEN coalesce(
                     contains(
                         CAST(json_extract(n.housing_json, '$.res_all') AS ARRAY(VARCHAR)),
                         CAST(n.class AS VARCHAR)
                     ), false
                 ) THEN 'res_all'
            ELSE NULL
        END AS desired_housing_key
    FROM normalized_json n
),

-- Choose a housing key that actually exists under tri{n} in stat_groups_json
effective_key AS (
    SELECT
      tri_and_key.*,

      -- test availability for desired key, then common fallbacks
      CASE
        WHEN json_array_length(json_extract(tri_and_key.stat_groups_json, format('$.tri%s.%s.columns', tri_and_key.tri_num, tri_and_key.desired_housing_key))) IS NOT NULL
          THEN tri_and_key.desired_housing_key
        WHEN json_array_length(json_extract(tri_and_key.stat_groups_json, format('$.tri%s.%s.columns', tri_and_key.tri_num, 'res_all'))) IS NOT NULL
          THEN 'res_all'
        WHEN json_array_length(json_extract(tri_and_key.stat_groups_json, format('$.tri%s.%s.columns', tri_and_key.tri_num, 'condos'))) IS NOT NULL
          THEN 'condos'
        ELSE NULL
      END AS effective_housing_key
    FROM triad_and_key tri_and_key
),

-- Pull columns array for tri{n}.{effective_housing_key}.columns
cols_json AS (
    SELECT
        e_key.*,
        CASE
            WHEN e_key.tri_num IS NOT NULL AND e_key.effective_housing_key IS NOT NULL
                THEN json_extract(
                         e_key.stat_groups_json,
                         format('$.tri%s.%s.columns', e_key.tri_num, e_key.effective_housing_key)
                     )
            ELSE NULL
        END AS columns_json
    FROM effective_key e_key
)

SELECT
    meta_sale_document_num,
    run_id,
    meets_group_threshold,
    "group",
    group_size,
    --sv_price_deviation,
    --sv_price_per_sqft_deviation,
    meta_sale_price_original,
    sv_is_outlier,
    ptax_flag_original,
    sv_is_ptax_outlier,
    sv_outlier_reason1,
    sv_outlier_reason2,
    sv_outlier_reason3,

    pin,
    "year",
    triad_code,
    class,

    -- Use correct scalar/object extraction via parent-array indexing
    CASE
        WHEN columns_json IS NULL THEN CAST(ARRAY[] AS ARRAY(VARCHAR))
        ELSE transform(
               sequence(0, coalesce(json_array_length(columns_json), 0) - 1),
               i -> coalesce(
                      -- object with a "column" key
                      json_extract_scalar(columns_json, format('$[%s].column', i)),
                      -- scalar string at that index
                      json_extract_scalar(columns_json, format('$[%s]', i))
                    )
             )
    END AS groups_used

FROM cols_json
