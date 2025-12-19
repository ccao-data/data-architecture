-- Copy of default.vw_pin_appeal that feeds the "Appeals" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    pin || year || case_no || hearing_type || CAST(subkey AS VARCHAR) AS row_id,
    pin,
    class,
    township_code,
    CAST(year AS INT) AS year,
    mailed_bldg,
    mailed_land,
    mailed_tot,
    certified_bldg,
    certified_land,
    certified_tot,
    case_no,
    appeal_type,
    CASE WHEN hearing_type = '1' THEN 'Assessor recommendation'
        WHEN hearing_type = '2' THEN 'Certificate of correction'
        WHEN hearing_type = 'A' THEN 'Current year appeal'
        WHEN hearing_type = 'C' THEN 'Current appeal & certificate of error'
        WHEN hearing_type = 'E' THEN 'Certificate of error only'
        WHEN
            hearing_type = 'F'
            THEN 'Smartfile exemption certificate of error filing'
        WHEN hearing_type = 'O' THEN 'Omitted assessment'
        WHEN hearing_type = 'R' THEN 'Re-review'
        WHEN hearing_type = 'S' THEN 'Specific objection'
        WHEN hearing_type = 'T' THEN 'Taxable'
        WHEN hearing_type = 'X' THEN 'Exempt'
        ELSE hearing_type
    END AS hearing_type,
    subkey,
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
