--- A view to generate the top 5 parcels in a given municipality and year by AV

/* This CTE removes historical PINs in reporting.vw_pin_township_class that are
not in reporting.vw_pin_value_long. We do this to make sure the samples we
derive the numerator and denominator from for pct_pin_w_value_in_group are the
same. These differences are inherent in the tables that feed these views
(iasworld.pardat and iasworld.asmt_all, respectively) and are data errors - not
emblematic of what portion of a municipality has actually progressed through an
assessment stage.

It does NOT remove PINs from the most recent year of
reporting.vw_pin_township_class since we expect differences based on how
iasworld.asmt_all is populated through the year as the assessment cycle
progresses. This means data errors caused by differences between iasworld.pardat
and iasworld.asmt_all won't be addressed in the most recent year. Unfortunately,
we can't know what those errors are (or if they even exist) until asmt_all has
at least one fully complete stage for a given year.

Starting in 2020 a small number of PINs are present in iasworld.asmt_all for
one or two but not all three stages of assessment when we would expect all three
stages to be present for said PINs. This is also a data error, but is NOT
addressed in this view and leads to a few instances where
pct_pin_w_value_in_group ends up being less than 1 when it should equal 1.
16-07-219-029-1032 missing a mailed value but having CCAO and BOR certified
values in 2021 is an example. */
WITH trimmed_town_class AS (
    SELECT vptc.*
    FROM {{ ref('reporting.vw_pin_township_class') }} AS vptc
    LEFT JOIN
        (
            SELECT DISTINCT
                pin,
                year
            FROM {{ ref('reporting.vw_pin_value_long') }}
        )
            AS pins
        ON vptc.pin = pins.pin
        AND vptc.year = pins.year
    WHERE pins.pin IS NOT NULL
        OR vptc.year
        = (SELECT MAX(year) FROM {{ ref('reporting.vw_pin_township_class') }})

),

/* Calculate the denominator for the pct_pin_w_value_in_group column.
reporting.vw_pin_township_class serves as the universe of yearly PINs we expect
to see in reporting.vw_pin_value_long. */
pin_counts AS (
    SELECT
        vptc.municipality_name AS municipality,
        vptc.year,
        COUNT(*) AS total_n
    FROM trimmed_town_class AS vptc
    WHERE vptc.municipality_name IS NOT NULL
    GROUP BY
        vptc.municipality_name,
        vptc.year
),

-- Choose most recent assessor value (ignore BOR)
most_recent_values AS (
    SELECT
        pin,
        year,
        oneyr_pri_board_tot AS prior_bor_av,
        COALESCE(certified_tot, mailed_tot, pre_mailed_tot) AS ccao_av,
        CASE
            WHEN
                (certified_tot IS NULL AND mailed_tot IS NULL)
                THEN 'pre-mailed'
            WHEN certified_tot IS NULL THEN 'mailed'
            ELSE 'certified'
        END AS ccao_stage_used,
        board_tot AS bor_av,
        CASE
            WHEN
                board_tot IS NOT NULL AND certified_tot IS NOT NULL
                THEN board_tot - certified_tot
        END AS nom_bor_change,
        CASE
            WHEN
                board_tot IS NOT NULL AND certified_tot > 0
                THEN CAST((board_tot - certified_tot) AS DOUBLE)
                / CAST(certified_tot AS DOUBLE)
        END AS per_bor_change
    FROM {{ ref('default.vw_pin_history') }}
),

-- Create ranks
top_5 AS (
    SELECT
        vptc.municipality_name AS municipality,
        mrv.year,
        RANK() OVER (
            PARTITION BY vptc.municipality_name, mrv.year
            ORDER BY mrv.ccao_av DESC
        ) AS rank,
        CONCAT(
            SUBSTR(mrv.pin, 1, 2),
            '-',
            SUBSTR(mrv.pin, 3, 2),
            '-',
            SUBSTR(mrv.pin, 5, 3),
            '-',
            SUBSTR(mrv.pin, 8, 3),
            '-',
            SUBSTR(mrv.pin, 11, 4)
        ) AS pin,
        vptc.major_class,
        vptc.class,
        mrv.prior_bor_av,
        mrv.ccao_av,
        mrv.ccao_stage_used,
        mrv.bor_av,
        mrv.nom_bor_change,
        mrv.per_bor_change,
        vpa.prop_address_full AS address,
        CASE
            WHEN vpa.prop_address_city_name = 'Mc Cook' THEN 'McCook' WHEN
                vpa.prop_address_city_name = 'Forestview'
                THEN 'Forest View'
            ELSE vpa.prop_address_city_name
        END AS city,
        vpa.mail_address_name AS taxpayer_name,
        COUNT() OVER (
            PARTITION BY vptc.municipality_name, mrv.year
        ) AS num_pin_w_value
    FROM most_recent_values AS mrv
    LEFT JOIN trimmed_town_class AS vptc
        ON mrv.pin = vptc.pin
        AND mrv.year = vptc.year
    LEFT JOIN {{ ref('default.vw_pin_address') }} AS vpa
        ON mrv.pin = vpa.pin
        AND mrv.year = vpa.year
)

-- Only keep top 5
SELECT
    top_5.*,
    pin_counts.total_n AS num_pin_total_in_group,
    CAST(top_5.num_pin_w_value AS DOUBLE)
    / CAST(pin_counts.total_n AS DOUBLE) AS pct_pin_w_value_in_group
FROM top_5
LEFT JOIN pin_counts
    ON top_5.year = pin_counts.year
    AND top_5.municipality = pin_counts.municipality
WHERE top_5.rank <= 5
    AND top_5.municipality IS NOT NULL
ORDER BY top_5.municipality, top_5.year, top_5.class, top_5.rank
