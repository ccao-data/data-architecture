-- View to collect pin-level exemptions from exdet. *Only* PINs with exemptions
-- are included in this view.

-- List of exemption types to pivot
{% set exes = [
    'exe_disabled', 'exe_freeze', 'exe_homeowner',
    'exe_longtime_homeowner', 'exe_senior', 'exe_muni_built', 'exe_vet_dis_lt50',
    'exe_vet_dis_50_69', 'exe_vet_dis_ge70', 'exe_vet_dis_100',
    'exe_vet_returning', 'exe_wwii'
] %}

-- The long view can include both a CofE value and a non-CofE value for a
-- given PIN/year/exemption. For this view, we want to deduplicate those cases
-- and only choose the CofE
WITH long_group_ranked AS (
    SELECT
        pin,
        year,
        exemption_type,
        exemption_amount,
        ROW_NUMBER() OVER (
            PARTITION BY pin, year, exemption_type
            ORDER BY is_cofe DESC
        ) AS rank
    FROM {{ ref('default.vw_pin_exe_long') }}
),

long AS (
    SELECT
        pin,
        year,
        exemption_type,
        exemption_amount
    FROM long_group_ranked
    WHERE rank = 1
)

-- Pivot long exemption view so each exemption gets its own column
SELECT
    pin,
    year,
{%- for exe_ in exes %}
    CAST(SUM(CASE
        WHEN exemption_type = '{{ exe_ }}' THEN exemption_amount ELSE 0
        END) AS INT)
        AS {{ exe_ }}{%- if not loop.last -%},{%- endif -%}
{% endfor %}
FROM long
GROUP BY pin, year
