-- Copy of default.vw_pin_appeal that feeds the "Appeals" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    pin || year || case_no AS row_id,
    pin,
    class,
    township_code,
    year,
    mailed_bldg,
    mailed_land,
    mailed_tot,
    certified_bldg,
    certified_land,
    certified_tot,
    case_no,
    appeal_type,
    change,
    reason_code1,
    reason_desc1,
    reason_code2,
    reason_desc2,
    reason_code3,
    reason_desc3,
    agent_code,
    agent_name,
    status
FROM {{ ref('default.vw_pin_appeal') }}
