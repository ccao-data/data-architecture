-- View that combines `sale.flag` and `sale.flag_override` to produce one
-- unified view of all sales validation information for a sale based on its
-- doc number
SELECT
    COALESCE(flag.doc_no, review.doc_no) AS doc_no,
    flag.sv_is_outlier IS NOT NULL AS has_flag,
    COALESCE(flag.sv_is_outlier, FALSE) AS flag_is_outlier,
    COALESCE(flag.sv_is_ptax_outlier, FALSE) AS flag_is_ptax_outlier,
    COALESCE(flag.sv_is_heuristic_outlier, FALSE) AS flag_is_heuristic_outlier,
    flag.sv_run_id AS flag_run_id,
    flag.sv_version AS flag_version,
    FILTER(
        ARRAY[
            flag.sv_outlier_reason1,
            flag.sv_outlier_reason2,
            flag.sv_outlier_reason3
        ],
        r -> r IS NOT NULL
    ) AS flag_outlier_reasons,
    NOT COALESCE(
        review.has_class_change IS NULL
        AND review.has_characteristic_change IS NULL
        AND review.is_arms_length IS NULL
        AND review.is_flip IS NULL,
        FALSE
    ) AS has_review,
    COALESCE(review.is_arms_length, FALSE) AS review_is_arms_length,
    COALESCE(review.is_flip, FALSE) AS review_is_flip,
    COALESCE(review.has_class_change, FALSE) AS review_has_class_change,
    COALESCE(review.has_characteristic_change, FALSE)
        AS review_has_characteristic_change,
    COALESCE(
        review.has_class_change
        OR review.has_characteristic_change = 'yes_major',
        FALSE
    ) AS review_has_major_characteristic_change
FROM {{ source('sale', 'flag') }} AS flag
FULL OUTER JOIN {{ source('sale', 'flag_override') }} AS review
    ON flag.doc_no = review.doc_no
