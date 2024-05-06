--- A view to generate the top 5 parcels in a given municipality and year by AV

/* Ensure every municipality/class/year has a row for every stage through
cross-joining. This is to make sure that combinations that do not yet
exist in iasworld.asmt_all for the current year will exist in the view, but have
largely empty columns. For example: even if no class 4s in the City of Chicago
have been mailed yet for the current assessment year, we would still like an
empty City of Chicago/class 4 row to exist for the mailed stage. */
WITH stages AS (

    SELECT 'mailed' AS stage
    UNION
    SELECT 'certified' AS stage

),

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
trimmed_town_class AS (
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
        stages.stage,
        COUNT(*) AS total_n
    FROM trimmed_town_class AS vptc
    CROSS JOIN stages
    WHERE vptc.municipality_name IS NOT NULL
    GROUP BY
        vptc.municipality_name,
        vptc.year,
        stages.stage
),

-- Choose most recent assessor value (ignore BOR)
most_recent_values AS (
    SELECT
        pin,
        year,
        COALESCE(certified_tot, mailed_tot) AS total_av,
        CASE
            WHEN certified_tot IS NULL THEN 'mailed'
            ELSE 'certified'
        END AS stage_used
    FROM {{ ref('default.vw_pin_value') }}
    WHERE certified_tot IS NOT NULL
        OR mailed_tot IS NOT NULL
),

-- Create ranks
top_5 AS (
    SELECT
        mrv.year,
        vptc.municipality_name AS municipality,
        vptc.triad_name AS triad,
        vptc.class,
        RANK() OVER (
            PARTITION BY vptc.municipality_name, mrv.year
            ORDER BY mrv.total_av DESC
        ) AS rank,
        mrv.pin,
        mrv.total_av,
        vpa.prop_address_full AS address,
        vpa.prop_address_city_name AS city,
        vpa.mail_address_name AS owner_name,
        mrv.stage_used,
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
    WHERE vptc.municipality_name IS NOT NULL
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
    AND top_5.stage_used = pin_counts.stage
WHERE top_5.rank <= 5
ORDER BY top_5.municipality, top_5.year, top_5.class, top_5.rank
