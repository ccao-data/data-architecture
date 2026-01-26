-- View to collect pin-level exemptions from exdet. *Only* PINs with exemptions
-- are included in this view.

-- List of exemption types to pivot
{% set exes = [
    'exe_disabled', 'exe_freeze', 'exe_homeowner',
    'exe_longtime_homeowner', 'exe_senior', 'exe_muni_built', 'exe_vet_dis_lt50',
    'exe_vet_dis_50_69', 'exe_vet_dis_ge70', 'exe_vet_dis_100',
    'exe_vet_returning', 'exe_wwii'
] %}

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
FROM {{ ref('default.vw_pin_exe_long') }}
GROUP BY pin, year
