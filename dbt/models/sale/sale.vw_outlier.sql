-- View that combines `sale.flag` and `sale.flag_review` to produce one
-- unified view of all sales validation information for a sale based on its
-- doc number

-- Start by combining the algorithmic flags with the human reviewer
-- determinations. To make boolean logic simpler (avoiding conditions
-- unexpectedly evaluating to NULL), we coalesce all of the determination fields
-- to FALSE in cases where the sale has not been reviewed or flagged, and
-- instead provide the boolean columns `has_review` and `has_flag` so that
-- consumers can disambiguate FALSE determinations from missing review/flags
WITH flag_and_review AS (
    SELECT
        COALESCE(flag.doc_no, review.doc_no) AS doc_no,
        flag.sv_is_outlier IS NOT NULL AS has_flag,
        COALESCE(flag.sv_is_outlier, FALSE) AS flag_is_outlier,
        COALESCE(flag.sv_is_ptax_outlier, FALSE) AS flag_is_ptax_outlier,
        COALESCE(flag.sv_is_heuristic_outlier, FALSE)
            AS flag_is_heuristic_outlier,
        flag.sv_outlier_reason1 AS flag_outlier_reason1,
        flag.sv_outlier_reason2 AS flag_outlier_reason2,
        flag.sv_outlier_reason3 AS flag_outlier_reason3,
        -- Convenience column that combines all flag reasons into one array
        -- for easier logic in queries that consume this CTE. We don't select
        -- this column in the output of this view because it's redundant
        FILTER(
            ARRAY[
                flag.sv_outlier_reason1,
                flag.sv_outlier_reason2,
                flag.sv_outlier_reason3
            ],
            r -> r IS NOT NULL
        ) AS flag_outlier_reasons,
        flag.run_id AS flag_run_id,
        flag.version AS flag_version,
        NOT COALESCE(
            review.has_class_change IS NULL
            AND review.has_characteristic_change IS NULL
            AND review.is_arms_length IS NULL
            AND review.is_flip IS NULL,
            FALSE
        ) AS has_review,
        COALESCE(review.is_arms_length, TRUE) AS review_is_arms_length,
        COALESCE(review.is_flip, FALSE) AS review_is_flip,
        COALESCE(review.has_class_change, FALSE) AS review_has_class_change,
        COALESCE(review.has_characteristic_change, 'no')
            AS review_has_characteristic_change,
        COALESCE(
            review.has_class_change
            OR review.has_characteristic_change = 'yes_major',
            FALSE
        ) AS review_has_major_characteristic_change,
        JSON_OBJECT( -- noqa: disable=CP02,RF02
            'is_arms_length' VALUE is_arms_length,
            'is_flip' VALUE is_flip,
            'has_class_change' VALUE has_class_change,
            'has_characteristic_change' VALUE has_characteristic_change
        ) AS review_json -- noqa: enable=CP02,RF02
    FROM {{ ref('sale.vw_flag') }} AS flag
    FULL OUTER JOIN {{ source('sale', 'flag_review') }} AS review
        ON flag.doc_no = review.doc_no
),

{% set price_outlier_reasons = [
    "High price",
    "High price per square foot",
    "Low price",
    "Low price per square foot",
] -%}

-- Compare algorithmic flags to human review in order to make a final
-- determination about whether the sale is an outlier. Since we need to explain
-- our outlier determinations to data consumers, we start by producing verbose
-- reasons for our outlier determinations, and then when we consume this CTE
-- we will cast these reasons to a boolean flag that is easier to filter
outlier_reason AS (
    SELECT
        *,
        CASE
            -- Our sales validation pipeline includes algorithmic sale flagging
            -- alongside human review. If a human reviewed the sale, we want to
            -- consider those results first, because we weight those
            -- determinations more strongly than the algorithmic flags
            WHEN has_review
                THEN
                CASE
                    -- If a reviewer found a major characteristic change, it
                    -- should always indicate an outlier, regardless of what
                    -- the algorithm found. This is because incorrect major
                    -- characteristics can bias our valuation models
                    WHEN review_has_major_characteristic_change
                        THEN
                        'Review: Major Characteristic Change'
                    -- Flips and non-arm's-length sales only indicate outliers
                    -- if the algorithm found the sale price to be unusual.
                    -- This is because flips and non-arm's-length sales are
                    -- only problematic if the sale is unrepresentative of the
                    -- market; if the flip brings the property up to standard
                    -- for the market, or if a non-arm's-length sale is close
                    -- to market price, then the information from that sale is
                    -- still useful for our valuation models
                    WHEN has_flag AND flag_is_outlier
                        THEN
                        CASE
                            WHEN review_is_flip
                                THEN
                                CASE
                                    {%- for price_outlier_reason in price_outlier_reasons %}  -- noqa: LT05
                                        WHEN CONTAINS(
                                                flag_outlier_reasons,
                                                '{{ price_outlier_reason }}'
                                            )
                                            THEN 'Review: Flip, Algorithm: {{ price_outlier_reason }}'  -- noqa: LT05
                                    {%- endfor %}
                                    ELSE 'Review: Flip'
                                END
                            WHEN NOT review_is_arms_length
                                THEN
                                CASE
                                    {%- for price_outlier_reason in price_outlier_reasons %}  -- noqa: LT05
                                        WHEN CONTAINS(
                                                flag_outlier_reasons,
                                                '{{ price_outlier_reason }}'
                                            )
                                            THEN 'Review: Non-Arms-Length, Algorithm: {{ price_outlier_reason }}'  -- noqa: LT05
                                    {%- endfor %}
                                    ELSE 'Review: Non-Arms-Length'
                                END
                            ELSE
                                -- If we reach this branch, then the reviewer
                                -- did not find anything unusual about the
                                -- sale, which means the reviewer and the
                                -- algorithm disagree about whether the sale
                                -- is an outlier. In these cases, we choose to
                                -- trust the reviewer's determination
                                'Review: Valid Sale'
                        END
                    ELSE
                        -- If we reach this branch, we can be confident that
                        -- the sale does not have major characteristic errors,
                        -- and that the sales algorithm did not flag the sale
                        -- as having an unusual sale price. In this case, it
                        -- doesn't matter if the reviewer found the sale to be
                        -- a flip or a non-arm's-length transaction, since the
                        -- sale is in a sense "typical" enough to contain
                        -- relevant information for our valuation models.
                        --
                        -- Note that a sale can also reach this branch if its
                        -- algorithmic flag group did not have enough sales in
                        -- it to reach statistical significance, or if the
                        -- algorithm has not evaluated this sale yet. Though
                        -- these two conditions are semantically different from
                        -- the case where the algorithm evaluated the sale and
                        -- found it to have a typical sale price (i.e. it is
                        -- not an algorithmic outlier), we lump all of these
                        -- conditions together for the purpose of this
                        -- conditional branch, since we want to default to
                        -- including a sale if we don't have enough information
                        -- to decide if its sale price was atypical
                        'Review: Valid Sale'
                END
            -- If a reviewer has not looked at the sale, but the algorithm
            -- has evaluated it, then we will use the algorithm's decision
            -- as the basis for our outlier determination. If neither a
            -- reviewer nor the algorithm has evaluated the sale, then the
            -- outer CASE statement will fall through and return null
            WHEN
                has_flag
                THEN
                CASE
                    WHEN flag_is_outlier
                        THEN
                        CASE
                            -- The validation algorithm produces its own
                            -- set of reasons, so if these exist, we want
                            -- to use them for the final outlier reason.
                            --
                            -- It shouldn't be possible for the algorithm
                            -- to flag the sale as an outlier without
                            -- providing reasons for that decision, but
                            -- if this ever happens against our
                            -- expectation, we want to avoid a trailing
                            -- comma in the output
                            WHEN CARDINALITY(flag_outlier_reasons) > 0
                                THEN
                                CONCAT(
                                    'Algorithm: Outlier Sale, ',
                                    ARRAY_JOIN(
                                        flag_outlier_reasons, ', '
                                    )
                                )
                            ELSE
                                'Algorithm: Outlier Sale'
                        END
                    ELSE
                        'Algorithm: Valid Sale'
                END
        END AS outlier_reason
    FROM flag_and_review
)

SELECT
    doc_no,
    has_flag,
    flag_is_outlier,
    flag_is_ptax_outlier,
    flag_is_heuristic_outlier,
    flag_outlier_reason1,
    flag_outlier_reason2,
    flag_outlier_reason3,
    flag_run_id,
    flag_version,
    has_review,
    review_is_arms_length,
    review_is_flip,
    review_has_class_change,
    review_has_characteristic_change,
    review_json,
    outlier_reason,
    -- Cast the verbose outlier reasons to a boolean flag for easier filtering.
    -- See the comments on the logic that produces the outlier reason column
    -- above to clarify why these particular values indicate outliers or
    -- valid sales
    CASE
        WHEN outlier_reason = 'Review: Major Characteristic Change'
            OR outlier_reason LIKE 'Review: Non-Arms-Length%'
            OR outlier_reason LIKE 'Review: Flip%'
            OR outlier_reason LIKE 'Algorithm: Outlier Sale%'
            THEN TRUE
        WHEN outlier_reason IN (
                'Review: Valid Sale',
                'Algorithm: Valid Sale'
            )
            THEN FALSE
        -- Default to considering the sale to be valid if neither a human
        -- reviewer nor our algorithm has evaluated the sale. This should
        -- only apply to sales that are so old they are not relevant for
        -- modeling, or sales that are later than our modeling lien date
        -- (in which case we do not need to flag or review them yet)
        WHEN outlier_reason IS NULL
            THEN FALSE
    END AS is_outlier
FROM outlier_reason
