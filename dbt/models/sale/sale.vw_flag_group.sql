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

-- Normalize JSON: try parsing as-is; if NULL, retry after swapping single quotes to double quotes
normalized_json AS (
    SELECT
        b.*,
        coalesce(
            try(json_parse(b.stat_groups)),
            try(json_parse(replace(b.stat_groups, '''', '"')))
        ) AS stat_groups_json,
        coalesce(
            try(json_parse(b.housing_market_class_codes)),
            try(json_parse(replace(b.housing_market_class_codes, '''', '"')))
        ) AS housing_json
    FROM base b
),

-- Determine triad number & desired housing key dynamically from housing_json
triad_and_key AS (
    SELECT
        n.*,

        -- robust: keep only digits from triad_code (e.g., "tri3", "3", " TRI-2 ")
        NULLIF(regexp_replace(lower(trim(CAST(n.triad_code AS VARCHAR))), '[^0-9]+', ''), '') AS tri_num,

        -- DYNAMIC: find the first key in housing_json whose array contains this class
        CASE
            WHEN cardinality(
                     map_keys(
                       map_filter(
                         CAST(n.housing_json AS MAP(VARCHAR, JSON)),
                         (k, v) -> contains(CAST(v AS ARRAY(VARCHAR)), CAST(n.class AS VARCHAR))
                       )
                     )
                 ) > 0
            THEN map_keys(
                   map_filter(
                     CAST(n.housing_json AS MAP(VARCHAR, JSON)),
                     (k, v) -> contains(CAST(v AS ARRAY(VARCHAR)), CAST(n.class AS VARCHAR))
                   )
                 )[1]
            ELSE NULL
        END AS desired_housing_key
    FROM normalized_json n
),

-- Choose an effective housing key: prefer desired; otherwise dynamically pick any key under tri{n} that has a .columns array
effective_key AS (
    SELECT
      x.*,

      CASE
        WHEN json_array_length(
               json_extract(
                 x.stat_groups_json,
                 format('$.tri%s.%s.columns', x.tri_num, x.desired_housing_key)
               )
             ) IS NOT NULL
        THEN x.desired_housing_key
        ELSE
          CASE
            WHEN cardinality(
                   map_keys(
                     map_filter(
                       CAST(
                         json_extract(x.stat_groups_json, format('$.tri%s', x.tri_num))
                         AS MAP(VARCHAR, JSON)
                       ),
                       (k, v) -> json_array_length(json_extract(v, '$.columns')) IS NOT NULL
                     )
                   )
                 ) > 0
            THEN map_keys(
                   map_filter(
                     CAST(
                       json_extract(x.stat_groups_json, format('$.tri%s', x.tri_num))
                       AS MAP(VARCHAR, JSON)
                     ),
                     (k, v) -> json_array_length(json_extract(v, '$.columns')) IS NOT NULL
                   )
                 )[1]
            ELSE NULL
          END
      END AS effective_housing_key
    FROM triad_and_key x
),

-- Pull columns array for tri{n}.{effective_housing_key}.columns
cols_json AS (
    SELECT
        e.*,
        CASE
            WHEN e.tri_num IS NOT NULL AND e.effective_housing_key IS NOT NULL
                THEN json_extract(
                         e.stat_groups_json,
                         format('$.tri%s.%s.columns', e.tri_num, e.effective_housing_key)
                     )
            ELSE NULL
        END AS columns_json
    FROM effective_key e
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

    -- Convert the columns array into an ARRAY<VARCHAR> of column names,
    -- handling both scalar strings and objects with {"column": "..."}
    CASE
        WHEN columns_json IS NULL OR json_array_length(columns_json) = 0
        THEN CAST(ARRAY[] AS ARRAY(VARCHAR))
        ELSE transform(
               sequence(0, json_array_length(columns_json) - 1),
               i -> coalesce(
                      json_extract_scalar(columns_json, format('$[%s].column', i)),
                      json_extract_scalar(columns_json, format('$[%s]', i))
                    )
             )
    END AS groups_used

FROM cols_json;
