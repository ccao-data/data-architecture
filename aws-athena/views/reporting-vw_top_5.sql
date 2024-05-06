--- A view to generate the top 5 parcels in a given township and year by AV

-- Choose most recent assessor value (ignore BOR)
WITH most_recent_values AS (
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
        vptc.township_name AS township,
        vptc.triad_name AS triad,
        vptc.class,
        RANK() OVER (
            PARTITION BY vptc.township_code, mrv.year
            ORDER BY mrv.total_av DESC
        ) AS rank,
        mrv.pin,
        mrv.total_av,
        vpa.prop_address_full AS address,
        vpa.prop_address_city_name AS city,
        mrv.stage_used
    FROM most_recent_values AS mrv
    LEFT JOIN {{ ref('reporting.vw_pin_township_class') }} AS vptc
        ON mrv.pin = vptc.pin
        AND mrv.year = vptc.year
    LEFT JOIN {{ ref('default.vw_pin_address') }} AS vpa
        ON mrv.pin = vpa.pin
        AND mrv.year = vpa.year
    WHERE vptc.township_name IS NOT NULL
)

-- Only keep top 5
SELECT *
FROM top_5
WHERE rank <= 5
ORDER BY township, year, class, rank
