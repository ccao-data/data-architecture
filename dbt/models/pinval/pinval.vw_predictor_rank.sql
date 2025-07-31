{%- set predictors = all_predictors() -%}
-- Get some metadata for the model runs that we want to use as the basis for
-- pulling SHAP values for cards. This list of runs may be different from the
-- runs we use in `pinval.vw_assessment_card` or `pinval.vw_comp` because SHAPs
-- may come from different model runs in a year
WITH runs_to_include AS (
    SELECT
        meta.run_id,
        meta.assessment_year,
        COALESCE(
            final.township_code_coverage,
            -- `township_code_coverage` will only be present if the model is a
            -- final model, but it's possible for SHAP runs to be separate from
            -- the final model run. Handle this using a special indicator 'all'
            -- to mark SHAP runs that are not final models
            ARRAY['all']
        ) AS township_code_coverage
    FROM {{ source('model', 'metadata') }} AS meta
    LEFT JOIN {{ ref('model.final_model') }} AS final
        ON meta.run_id = final.run_id
    WHERE meta.run_id IN (
            '2024-02-06-relaxed-tristan',
            '2024-03-17-stupefied-maya',
            '2025-04-25-fancy-free-billy'
        )
),

-- Absolute values for SHAPs for all cards in PINVAL model runs
shap_abs AS (
    SELECT
        shap.run_id,
        shap.year,
        shap.township_code,
        shap.meta_pin,
        shap.meta_card_num
    {% for predictor in predictors -%}
            shap.{{ predictor }},
            abs(shap.{{ predictor }}) AS abs_{{ predictor }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source('model', 'shap') }} AS shap
    INNER JOIN (
        SELECT
            run.*,
            t.township_code
        FROM runs_to_include AS run
        -- Handle the use of different model runs for different towns
        CROSS JOIN UNNEST(run.township_code_coverage) AS t (township_code)
    ) AS run
        ON shap.run_id = run.run_id
        AND (
            shap.township_code = run.township_code
            -- Handle non-final SHAP models
            OR run.township_code = 'all'
        )
),

-- Sums of absolute values for SHAPs for all cards
shap_sum AS (
    SELECT
        run_id,
        year,
        township_code,
        meta_pin,
        meta_card_num,
        ({% for predictor in predictors -%}
            COALESCE(abs_{{ predictor }}, 0){% if not loop.last %} + {% endif %}
        {% endfor %}) AS shap_sum
    FROM shap_abs
),

-- Get magnitude of absolute SHAP value relative to the sum of absolute SHAP
-- values for a card, as a rough measure of feature importance for that card.
-- We'll consider this to be the predictor weight
pred_wt AS (
    SELECT
        shap_abs.run_id,
        shap_abs.year,
        shap_abs.township_code,
        shap_abs.meta_pin,
        shap_abs.meta_card_num,
        shap_sum.shap_sum
    {% for predictor in predictors -%}
            shap_abs.{{ predictor }},
            shap_abs.abs_{{ predictor }},
            shap_abs.abs_{{ predictor }} / shap_sum.shap_sum AS wt_{{ predictor }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM shap_abs
    INNER JOIN shap_sum
        ON shap_abs.run_id = shap_sum.run_id
        AND shap_abs.year = shap_sum.year
        AND shap_abs.township_code = shap_sum.township_code
        AND shap_abs.meta_pin = shap_sum.meta_pin
        AND shap_abs.meta_card_num = shap_sum.meta_card_num
),

-- Pull predictors and weights into arrays so that we can pivot them longer
-- in order to facilitate ranking them and computing quantiles
pred_wt_map AS (
    SELECT
        run_id,
        year,
        township_code,
        meta_pin,
        meta_card_num,
        MAP(
            ARRAY[
                {% for predictor in predictors -%}
                    'wt_{{ predictor }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            ],
            ARRAY[
                {% for predictor in predictors -%}
                    wt_{{ predictor }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            ]
        ) AS pred_wt_map
    FROM pred_wt
),

-- Pivot predictor weights longer
pred_wt_long AS (
    SELECT
        run_id,
        year,
        township_code,
        meta_pin,
        meta_card_num,
        pred_name,
        pred_wt
    FROM pred_wt_map
    CROSS JOIN UNNEST(pred_wt_map)
),

-- Compute rank and quantile for all predictors
pred_rank_long AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY run_id, year, township_code, meta_pin, meta_card_num
            ORDER BY pred_wt DESC
        ) AS rank,
        NTILE(3) OVER (
            PARTITION BY run_id, year, township_code, meta_pin, meta_card_num
            ORDER BY pred_wt ASC
        ) AS tercile
    FROM pred_wt_long
),

-- Pivot the weights, ranks, and quantiles wider so we can return one row per
-- card
pred_rank AS (
    SELECT
        run_id,
        year,
        township_code,
        meta_pin,
        meta_card_num
    {% for predictor in predictors -%}
            MAX(CASE WHEN pred_name = 'wt_{{ predictor }}' THEN pred_wt END) AS wt_{{ predictor }},
            MAX(CASE WHEN pred_name = 'wt_{{ predictor }}' THEN rank END) AS rank_{{ predictor }},
            MAX(CASE WHEN pred_name = 'wt_{{ predictor }}' THEN tercile END) AS terc_{{ predictor }}{% if not loop.last %},{% endif %}
        {% endfor -%}
    FROM pred_rank_long
    GROUP BY run_id, year, township_code, meta_pin, meta_card_num
)

SELECT *
FROM pred_rank
